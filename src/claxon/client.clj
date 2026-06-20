(ns claxon.client
  (:require
   [claxon.conf :as conf]
   [claxon.impl :as i])
  (:import
   [java.io BufferedInputStream BufferedWriter OutputStreamWriter]
   [java.net InetSocketAddress Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent ExecutorService]))

(defn add-handler
  [conn handler {:keys [op args]}]
  (swap! i/handlers
         conj
         {:conn (:id conn)
          :fn handler
          :matches {:op op :args args}}))

(defn connect
  ([]
   (connect {}))
  ([opts]
   (let [opts (merge (conf/defaults) opts)
         {:keys [claxon/urls
                 claxon/timeout-ms
                 claxon/executor
                 claxon/handlers
                 claxon/frame-shapes]} opts
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
         conn {:id (swap! i/conn-ids inc)
               :socket sock
               :reader in
               :writer out
               :executor executor
               :frame-shapes frame-shapes}]
     (run! (fn [[{:keys [op args]} f]]
             (add-handler conn f {:op op :args args}))
           handlers)
     (i/start conn)
     (i/snd conn "CONNECT" (i/write-json (dissoc opts
                                                 :claxon/urls
                                                 :claxon/timeout-ms
                                                 :claxon/executor
                                                 :claxon/handlers
                                                 :claxon/frame-shapes)))
     conn)))

(defn close
  [{:keys [socket executor id]}]
  (swap! i/handlers (fn [h] (vec (remove #(= (:conn %) id) h))))
  (ExecutorService/.shutdown executor)
  (Socket/.close socket))

(comment
  (set! *warn-on-reflection* true)

  (def conn (connect))

  (i/snd conn "PING")

  (close conn))
