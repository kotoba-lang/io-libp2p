(ns kotoba.net.libp2p.pubsub-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotoba.net.libp2p.pubsub :as pubsub]))

(deftest standard-rpc-protobuf-fixtures
  (is (= [10 6 8 1 18 2 47 120]
         (pubsub/encode-rpc
          {:subscriptions [(pubsub/subscription "/x" true)]})))
  (is (= [18 8 18 2 1 2 34 2 47 120]
         (pubsub/encode-rpc
          {:messages [(pubsub/strict-no-sign-message "/x" [1 2])]})))
  (is (= {:subscriptions [{:subscribe? true :topic "/x"}]
          :messages []}
         (pubsub/decode-rpc [10 6 8 1 18 2 47 120]))))

(deftest strict-no-sign-rejects-pubsub-authorship-fields
  (testing "IPNS authenticates its own record, never a forged PubSub author"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"not-strict-no-sign"
         (pubsub/encode-rpc
          {:messages [{:topic "/record/x" :data [1] :from [9]}]})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"not-strict-no-sign"
         (pubsub/decode-rpc [18 9 10 1 9 18 1 1 34 1 120])))))
