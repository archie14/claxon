# Examples and recipes

The best way to know what each option's type and values could be is to refer the [Protocol Docs](https://docs.nats.io/reference/reference-protocols/nats-protocol) directly.
All values would be passed as described.

**Found a newer, neater way to do something? Or just something new and useful? Please consider contributing here!**

### publish and subscribe

```clojure
(require '[claxon.client :as nats])

(def conn (nats/connect))

(nats/add-handler conn
  (fn [frame _conn]
    (println "received:" (String. (:body frame) "UTF-8")))
  {:op "MSG" :args {:subject "demo"}})

(nats/invoke conn {:op "SUB" :args {:subject "demo" :sid "1"}})
(nats/invoke conn {:op "PUB" :args {:subject "demo"} :payloads {:body "hello, nats"}})

;; => prints "received: hello, nats" shortly after, on the reader thread

(nats/close conn)
```

### headers (HPUB / HMSG)

```clojure
(nats/add-handler conn
  (fn [frame _conn]
    (println "headers:" (get-in frame [:headers :headers]))
    (println "body:" (String. (:body frame) "UTF-8")))
  {:op "HMSG" :args {:subject "demo.headers"}})

(nats/invoke conn {:op "SUB" :args {:subject "demo.headers" :sid "2"}})

(nats/invoke conn
  {:op "HPUB"
   :args {:subject "demo.headers"}
   :payloads {:headers {:headers {"Content-Type" ["text/plain"]}}
              :body "with headers this time"}})
```

When sending, header keys can be keywords (`:Content-Type`) as well as strings, and a single value doesn't need to be wrapped in a vector (`"text/plain"` works as well as `["text/plain"]`).
Incoming headers are always parsed back as `{<string> [<string> ...]}`, regardless of how they were written.

### JetStream and KV recipes

claxon has no JetStream or KV specific functions yet, all are implemented on the NATS server as regular subjects (`$JS.API.*` for management, `$KV.<bucket>.<key>` for KV) that you talk to with ordinary `PUB`/`SUB`/`HPUB`.
The examples below show that explicitly, including the request/reply pattern (subscribe to an inbox, publish with `:reply-to` set to it) that claxon doesn't wrap for you.

A small helper to simplify the request/reply dance, not something claxon ships (yet):

```clojure
(defn request
  "Send `body` to `subject` and block (up to `timeout-ms`) for a single reply."
  [conn subject body timeout-ms]
  (let [inbox (str "_INBOX." (random-uuid))
        p (promise)
        hid (add-handler conn
                         (fn [frame _conn] (deliver p frame))
                         {:op "MSG" :args {:subject inbox}})]
    (invoke conn {:op "SUB" :args {:subject inbox :sid inbox}})
    (invoke conn {:op "PUB"
                  :args {:subject subject :reply-to inbox}
                  :payloads {:body body}})
    (let [frame (deref p timeout-ms :timeout)]
      (remove-handler conn hid)
      (invoke conn {:op "UNSUB" :args {:sid inbox}})
      frame)))
```

#### Creating a stream and a durable pull consumer

```clojure
(require '[clojure.data.json :as json]) ; or cheshire.core on bb

(request conn "$JS.API.STREAM.CREATE.ORDERS"
         (json/write-str {"name" "ORDERS" "subjects" ["ORDERS.*"]})
         2000)

(request conn "$JS.API.CONSUMER.DURABLE.CREATE.ORDERS.PULLER"
         (json/write-str {"durable_name" "PULLER"
                          "ack_policy" "explicit"})
         2000)
```

#### Publishing into the stream

Publishing to a JetStream backed subject is just a `PUB`, the server intercepts it because the subject matches a stream:

```clojure
(nats/invoke conn {:op "PUB" :args {:subject "ORDERS.new"} :payloads {:body "order #1"}})
```

#### Pulling and acking from the consumer

A pull request is a `request` to `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>`, the reply's own `:reply-to` is the ack subject and publishing an empty body there acks the message:

```clojure
(let [msg (request conn "$JS.API.CONSUMER.MSG.NEXT.ORDERS.PULLER" "1" 5000)]
  (println "got:" (String. (:body msg) "UTF-8"))
  ;; ack it by publishing nothing to its reply-to
  (nats/invoke conn {:op "PUB" :args {:subject (get-in msg [:args :reply-to])}}))
```

#### A simple KV bucket

A KV bucket is just a stream named `KV_<bucket>` whose subjects look like `$KV.<bucket>.<key>`:

```clojure
;; create the bucket (a stream under the hood)
(request conn "$JS.API.STREAM.CREATE.KV_profiles"
         (json/write-str {"name" "KV_profiles" "subjects" ["$KV.profiles.>"]})
         2000)

;; put: publish the value to the key's subject
(nats/invoke conn {:op "PUB"
                   :args {:subject "$KV.profiles.sue"}
                   :payloads {:body "{\"color\":\"blue\"}"}})

;; get: ask the stream for the last message on that subject.
;; The reply is a JSON envelope, the actual value is base64-encoded
;; inside response["message"]["data"], not the raw body.
(let [resp (request conn "$JS.API.STREAM.MSG.GET.KV_profiles"
                     (json/write-str {"last_by_subj" "$KV.profiles.sue"})
                     2000)
      parsed (json/read-str (String. (:body resp) "UTF-8"))
      value (String. (.decode (java.util.Base64/getDecoder)
                               ^String (get-in parsed ["message" "data"]))
                      "UTF-8")]
  (println value)) ;; => {"color":"blue"}

;; delete: publish an empty body with a KV-Operation: DEL header
(nats/invoke conn {:op "HPUB"
                   :args {:subject "$KV.profiles.sue"}
                   :payloads {:headers {:headers {"KV-Operation" ["DEL"]}}
                              :body nil}})
```

### queueing (replacing something like RabbitMQ)

NATS has two different mechanisms for "queuing" and they give very different guarantees. Picking the right one matters if you're replacing a broker like RabbitMQ that you expect to hold messages durably and retry failed work.

#### Queue groups: load balancing, no durability

A queue group is a label on a `SUB`, the server picks one subscriber in the group per message instead of fanning out to all of them.
There's no storage involved. If nobody's subscribed when a message is published, it's gone, same as any other core NATS subject.

```clojure
;; start two "workers" in the same queue group: each PUB to "jobs" goes to
;; exactly one of them, round-robin, not both
(doseq [worker-id ["worker-1" "worker-2"]]
  (nats/add-handler conn
    (fn [frame _conn]
      (println worker-id "got:" (String. (:body frame) "UTF-8")))
    {:op "MSG" :args {:subject "jobs"}}))

(nats/invoke conn {:op "SUB" :args {:subject "jobs" :queue-group "workers" :sid "w1"}})
(nats/invoke conn {:op "SUB" :args {:subject "jobs" :queue-group "workers" :sid "w2"}})

(dotimes [i 5]
  (nats/invoke conn {:op "PUB" :args {:subject "jobs"} :payloads {:body (str "job " i)}}))
```

This is good for load-balancing fire-and-forget work where losing a message on a crash is acceptable.
It is **not** a RabbitMQ replacement on its own: there's no persistence, no ack, no redelivery.

#### JetStream work-queue stream: the actual RabbitMQ-equivalent

For RabbitMQ-style guarantees (messages survive until a worker successfully processes them, failed work gets retried) you want a JetStream stream with `"retention": "workqueue"` and a durable pull consumer.
The `request` helper is the same one defined in the sections above.

```clojure
;; create a work-queue stream: each message is delivered to exactly one
;; consumer and removed from the stream as soon as it's acked
(request conn "$JS.API.STREAM.CREATE.JOBS"
         (json/write-str {"name" "JOBS"
                          "subjects" ["jobs.>"]
                          "retention" "workqueue"})
         2000)

;; a single durable consumer, shared by every worker process —
;; work-queue streams only allow one (non-overlapping) consumer per subject,
;; so this is how you fan work out across many workers, not separate consumers
(request conn "$JS.API.CONSUMER.DURABLE.CREATE.JOBS.WORKERS"
         (json/write-str {"durable_name" "WORKERS"
                          "ack_policy" "explicit"
                          "ack_wait" 30000000000}) ; 30s, in nanoseconds
         2000)
```

Each worker pulls one message at a time, processes it, and acks or nacks:

```clojure
(defn run-worker [conn worker-id]
  (future
    (loop []
      (let [msg (request conn "$JS.API.CONSUMER.MSG.NEXT.JOBS.WORKERS" "1" 5000)]
        (when (not= msg :timeout)
          (let [reply-to (get-in msg [:args :reply-to])
                body (String. (:body msg) "UTF-8")]
            (try
              (println worker-id "processing:" body)
              ;; ... do the actual work here ...
              (nats/invoke conn {:op "PUB" :args {:subject reply-to}}) ; +ACK
              (catch Exception _
                ;; ask the server to redeliver this message
                (nats/invoke conn {:op "PUB" :args {:subject reply-to}
                                   :payloads {:body "-NAK"}}))))))
      (recur))))

(run-worker conn "worker-1")
(run-worker conn "worker-2")

(dotimes [i 5]
  (nats/invoke conn {:op "PUB" :args {:subject "jobs.new"} :payloads {:body (str "job " i)}}))
```

A few things to note, since this is what makes it different from the queue group above:

- Unacked messages are automatically redelivered after `ack_wait` (30s here), to whichever worker pulls next. No message is lost if a worker dies mid-job.
- A worker can actively reject a message (`-NAK`) to put it back for retry sooner than the ack-wait timeout, as shown in the `catch` above.
- Once a message is acked, `workqueue` retention deletes it from the stream: it's a true queue, not a log you replay.
- If you need a dead-letter queue, set `"max_deliver"` on the consumer config and watch `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.JOBS.WORKERS` for messages that exhausted their retries.
