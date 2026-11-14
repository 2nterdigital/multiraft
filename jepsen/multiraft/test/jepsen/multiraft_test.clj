(ns jepsen.multiraft-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.multiraft.client :as client]
            [jepsen.multiraft.cluster :as cluster]
            [jepsen.multiraft.nemesis :as nemesis]
            [clojure.java.shell :as shell]))

(deftest admin-url-mapping
  (let [test {:base-port 23000}]
    (is (= "http://127.0.0.1:23100" (client/admin-url test "1")))
    (is (= "http://127.0.0.1:23101" (client/admin-url test "2")))
    (is (= "http://127.0.0.1:23102" (client/admin-url test 3)))))

(deftest descriptor-admin-url-and-voters
  (with-redefs [cluster/load-descriptor
                (constantly {:base_port 25000
                             :nodes [{:id 1 :role "voter" :admin_url "http://127.0.0.1:25123"}
                                     {:id 2 :role "voter"}
                                     {:id 3 :role "standby"}]})]
    (is (= "http://127.0.0.1:25123" (client/admin-url {} "1")))
    (is (= "http://127.0.0.1:25101" (client/admin-url {} "2")))
    (is (= ["1" "2"] (client/nodes {:nodes ["1" "2" "3"]})))))

(deftest restart-keeps-relative-path-context
  (let [invocation (atom nil)
        data "../../.tmp/slot-foundation/relative-data"
        bin "../../.tmp/slot-foundation/fake-demo"]
    (with-redefs-fn {#'nemesis/env-or (fn [k default]
                                      (if (= k "MULTIRAFT_ROOT") "../.." default))
                    #'nemesis/read-pid (fn [_test _id] 123)
                    #'shell/sh (fn [& args]
                                 (reset! invocation args)
                                 {:exit 0 :out "" :err ""})}
      #(is (= {:started "1" :pid 123}
              (#'nemesis/start-node! {:data-dir data :demo-bin bin :nodes ["1" "2" "3"]} "1"))))
    (let [[bash script id role & options] @invocation
          opts (apply hash-map options)]
      (is (= ["bash" "1" "voter"] [bash id role]))
      (is (.isAbsolute (java.io.File. script)))
      (is (nil? (:dir opts)))
      (is (= data (get-in opts [:env "DATA_DIR"])))
      (is (= bin (get-in opts [:env "DEMO_BIN"]))))))
