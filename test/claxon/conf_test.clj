(ns claxon.conf-test
  (:require
   [claxon.conf :as conf]
   [clojure.test :refer [deftest is]]))

(deftest test-frame-shapes-completeness
  (let [expected-ops #{"INFO"
                       "CONNECT"
                       "PUB"
                       "HPUB"
                       "MSG"
                       "HMSG"
                       "SUB"
                       "UNSUB"
                       "PING"
                       "PONG"
                       "+OK"
                       "-ERR"}]
    (is (= expected-ops (set (keys (:claxon/frame-shapes (conf/defaults))))))))
