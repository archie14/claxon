(ns claxon.client
  (:require
   [claxon.impl :as i])
  (:import
   [java.io BufferedInputStream BufferedWriter OutputStreamWriter]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent ExecutorService Executors]))

(defn add-handler
  [handler {:keys [cmd args]}]
  (let [id (swap! i/handler-ids inc)]
    (swap! i/handlers assoc id {:fn handler :matches {:cnd cmd :args args}})))

(defn connect
  [{:keys [claxon/urls claxon/timeout-ms claxon/executor] :as opts}]
  (let [urls (or urls ["nats://localhost:4222"])
        timeout-ms (or timeout-ms 2000)
        ^Socket sock (->> urls
                          (map i/parse-nats-url)
                          (shuffle)
                          (some (fn [{:keys [^String host ^Integer port]}]
                                  (try
                                    (doto (Socket.)
                                      (.connect (InetSocketAddress. host port)
                                                timeout-ms))
                                    (catch Exception _ false)))))
        _ (when-not sock
            (throw (ex-info "Cannot connect to any of the urls" {:urls urls})))
        in (-> sock
               .getInputStream
               BufferedInputStream.)
        out (-> sock
                .getOutputStream
                (OutputStreamWriter. StandardCharsets/UTF_8)
                BufferedWriter.)
        conn {:socket sock
              :reader in
              :writer out
              :executor (or executor (Executors/newVirtualThreadPerTaskExecutor))}]
    ;; TODO: CONNECT with opts
    (i/start conn)
    conn))

;; TODO: draw the rest of the owl

(defn close
  [{:keys [socket executor]}]
  (ExecutorService/.shutdown executor)
  (Socket/.close socket))

(comment
  (set! *warn-on-reflection* true)

  (def conn (connect {}))

  (add-handler #() {})

  (i/snd conn "PING")

  (close conn))
