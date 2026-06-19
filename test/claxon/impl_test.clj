(ns claxon.impl-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [claxon.impl :as impl])
  (:import
   [java.io ByteArrayInputStream]))

(defn ->stream [s]
  (ByteArrayInputStream. (.getBytes s "UTF-8")))

(defn ->stream-with-payload [header payload]
  (let [header-bytes (.getBytes header "UTF-8")
        hlen (count header-bytes)
        plen (count payload)
        total (byte-array (+ hlen plen 2))]
    (System/arraycopy header-bytes 0 total 0 hlen)
    (System/arraycopy payload 0 total hlen plen)
    (aset-byte total (+ hlen plen) 13)
    (aset-byte total (+ hlen plen 1) 10)
    (ByteArrayInputStream. total)))

(defn read-frame-from-string [s]
  (impl/read-frame (->stream s)))

(defn read-frame-with-payload [header payload]
  (impl/read-frame (->stream-with-payload header payload)))

(def frame-cases
  [{:name "PING"
    :input "PING\r\n"
    :expected {:op "PING"}}
   {:name "PONG"
    :input "PONG\r\n"
    :expected {:op "PONG"}}
   {:name "+OK"
    :input "+OK\r\n"
    :expected {:op "+OK"}}
   {:name "-ERR"
    :input "-ERR 'bad'\r\n"
    :expected {:op "-ERR" :args {:msg "'bad'"}}}
   {:name "INFO"
    :input "INFO {\"server\":\"test\"}\r\n"
    :expected {:op "INFO" :args {:info {"server" "test"}}}}
   {:name "SUB"
    :input "SUB foo 1\r\n"
    :expected {:op "SUB" :args {:subject "foo" :sid "1"}}}
   {:name "SUB w/queue"
    :input "SUB foo bar 1\r\n"
    :expected {:op "SUB" :args {:subject "foo" :queue-group "bar" :sid "1"}}}
   {:name "UNSUB"
    :input "UNSUB 1\r\n"
    :expected {:op "UNSUB" :args {:sid "1"}}}
   {:name "UNSUB w/max"
    :input "UNSUB 1 10\r\n"
    :expected {:op "UNSUB" :args {:sid "1" :max-msgs 10}}}
   {:name "CONNECT"
    :input "CONNECT {\"verbose\":true}\r\n"
    :expected {:op "CONNECT" :args {:opts {"verbose" true}}}}
   {:name "PUB"
    :header "PUB foo 5\r\n"
    :payload (byte-array (map byte "hello"))
    :expected {:op "PUB" :args {:subject "foo" :bytes 5} :body [104 101 108 108 111]}}
   {:name "PUB w/reply"
    :header "PUB foo bar 5\r\n"
    :payload (byte-array (map byte "hello"))
    :expected {:op "PUB" :args {:subject "foo" :reply-to "bar" :bytes 5} :body [104 101 108 108 111]}}
   {:name "MSG"
    :header "MSG foo 1 5\r\n"
    :payload (byte-array (map byte "hello"))
    :expected {:op "MSG" :args {:subject "foo" :sid "1" :bytes 5} :body [104 101 108 108 111]}}
   {:name "MSG w/reply"
    :header "MSG foo 1 bar 5\r\n"
    :payload (byte-array (map byte "hello"))
    :expected {:op "MSG" :args {:subject "foo" :sid "1" :reply-to "bar" :bytes 5} :body [104 101 108 108 111]}}])

(deftest test-frame-parsing
  (testing "All core frames parse correctly"
    (doseq [{:keys [name input header payload expected]} frame-cases]
      (let [result (if payload
                     (read-frame-with-payload header payload)
                     (read-frame-from-string input))
            ;; Convert actual body to vector for comparison, if present
            actual (if-let [body (:body result)]
                     (assoc result :body (vec body))
                     result)]
        (is (= expected actual) (str "Failed on: " name))))))

(def header-bytes (byte-array (.getBytes "NATS/1.0\r\nX-Custom: value\r\n\r\n" "UTF-8")))
(def body-bytes (byte-array (map byte "hello")))
(def all-payload (byte-array (concat header-bytes body-bytes)))
(def header-len (count header-bytes))
(def body-len (count body-bytes))
(def total-len (+ header-len body-len))
(def expected-body-vec (vec body-bytes))

(deftest test-header-frames
  (testing "HPUB frame with headers"
    (let [header-str (str "HPUB foo " header-len " " total-len "\r\n")
          result (read-frame-with-payload header-str all-payload)]
      (is (= "HPUB" (:op result)))
      (is (= {:subject "foo" :hdr-bytes header-len :bytes total-len}
             (:args result)))
      (is (= {:headers {"X-Custom" ["value"]}} (:headers result)))
      (is (= expected-body-vec (vec (:body result))))))

  (testing "HMSG frame with headers and reply-to"
    (let [header-str (str "HMSG foo 1 bar " header-len " " total-len "\r\n")
          result (read-frame-with-payload header-str all-payload)]
      (is (= "HMSG" (:op result)))
      (is (= {:subject "foo" :sid "1" :reply-to "bar"
              :hdr-bytes header-len :bytes total-len}
             (:args result)))
      (is (= {:headers {"X-Custom" ["value"]}} (:headers result)))
      (is (= expected-body-vec (vec (:body result)))))))

(deftest test-parse-errors
  (testing "Unknown operation"
    (is (thrown? Exception (read-frame-from-string "UNKNOWN\r\n"))))

  (testing "Incomplete line (missing CRLF)"
    (is (thrown? java.io.EOFException (impl/read-all (->stream "PING")))))

  (testing "Wrong argument count"
    (is (thrown? Exception (read-frame-from-string "SUB foo\r\n"))))

  (testing "Invalid CRLF after payload"
    (let [stream (ByteArrayInputStream.
                  (byte-array (concat (.getBytes "PUB foo 5\r\n" "UTF-8")
                                      (map byte "hello")
                                      [10])))] ; only LF, no CR
      (is (thrown? Exception (impl/read-frame stream))))))

(deftest test-multi-frame
  (let [frames ["PING\r\n" "PONG\r\n"]
        stream (ByteArrayInputStream. (.. (apply str frames) (getBytes "UTF-8")))]
    (is (= {:op "PING"} (impl/read-frame stream)))
    (is (= {:op "PONG"} (impl/read-frame stream)))))

(deftest test-parse-headers-block
  (let [raw (byte-array (.getBytes "NATS/1.0 200 OK\r\nX-Foo: bar\r\n\r\n" "UTF-8"))]
    (is (= {:headers {"X-Foo" ["bar"]} :status 200 :description "OK"}
           (impl/parse-headers-block raw)))))

(deftest test-eval-length
  (testing "Literal numbers"
    (is (= 5 (impl/eval-length 5 {}))))
  (testing "Keyword lookup"
    (is (= 10 (impl/eval-length :bytes {:bytes 10}))))
  (testing "Subtraction with keywords"
    (is (= 7 (impl/eval-length [:- :total :used] {:total 10 :used 3}))))
  (testing "Addition with keywords"
    (is (= 15 (impl/eval-length [:+ :a :b] {:a 10 :b 5}))))
  (testing "Mix of literals and keywords"
    (is (= 8 (impl/eval-length [:+ 3 :x] {:x 5})))
    (is (= 2 (impl/eval-length [:- :total 8] {:total 10}))))
  (testing "Realistic usage from protocol"
    (is (= 5 (impl/eval-length [:- :hdr-bytes :bytes] {:hdr-bytes 10 :bytes 5})))))

(deftest test-frame-shapes-completeness
  (let [expected-ops #{"INFO" "CONNECT" "PUB" "HPUB" "MSG" "HMSG"
                       "SUB" "UNSUB" "PING" "PONG" "+OK" "-ERR"}]
    (is (= expected-ops (set (keys impl/frame-shapes))))))
