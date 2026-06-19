(ns claxon.client
  (:require
   [claxon.impl :as i])
  (:import
   [java.io BufferedInputStream BufferedWriter OutputStreamWriter]
   [java.net Socket]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent Executors]))

(defn connect
  [{:keys [^String host port claxon/executor] :as opts}]
  (let [host (or host "localhost")
        port (or port 4222)
        sock (Socket. host (int port))
        in (-> sock
               .getInputStream
               BufferedInputStream.)
        out (-> sock
                .getOutputStream
                (OutputStreamWriter. StandardCharsets/UTF_8)
                BufferedWriter.)
        conn {:socket sock :reader in :writer out}]
    ;; TODO: CONNECT with opts
    (i/start in (or executor (Executors/newVirtualThreadPerTaskExecutor)))
    conn))

(defn close
  [{:keys [socket]}]
  (Socket/.close socket))

(comment
  (set! *warn-on-reflection* true)

  (def conn (connect {}))

  (i/snd conn "PING")

  (close conn))
