(ns kotoba.net.libp2p.pubsub
  "The interoperable protobuf envelope used by libp2p PubSub/GossipSub.

  This namespace owns the wire contract, not the mesh algorithm.  IPNS records
  use StrictNoSign: their own IPNS signature is authoritative, while PubSub's
  optional `from`, `seqno`, `signature`, and `key` fields must be absent."
  (:require [protobuf.wire :as pb]))

(def gossipsub-v1-1 "/meshsub/1.1.0")
(def max-rpc-size (* 1024 1024))

(def subscription-schema
  {1 {:name :subscribe? :type :bool}
   2 {:name :topic :type :string}})

(def message-schema
  {1 {:name :from :type :bytes}
   2 {:name :data :type :bytes}
   3 {:name :seqno :type :bytes}
   4 {:name :topic :type :string}
   5 {:name :signature :type :bytes}
   6 {:name :key :type :bytes}})

(def rpc-schema
  {1 {:name :subscriptions :type :message :schema subscription-schema :repeated true}
   2 {:name :messages :type :message :schema message-schema :repeated true}})

(defn- fail! [problem data]
  (throw (ex-info (name problem) (assoc data :problem problem))))

(defn subscription [topic subscribe?]
  {:topic topic :subscribe? (boolean subscribe?)})

(defn strict-no-sign-message [topic data]
  {:topic topic :data (vec data)})

(defn strict-no-sign? [message]
  (and (string? (:topic message))
       (some? (:data message))
       (every? #(nil? (get message %)) [:from :seqno :signature :key])))

(defn encode-rpc [{:keys [subscriptions messages]}]
  (when-not (every? strict-no-sign? (or messages []))
    (fail! :pubsub/not-strict-no-sign {}))
  (let [wire (pb/encode rpc-schema
                        {:subscriptions (vec (or subscriptions []))
                         :messages (vec (or messages []))})]
    (when (> (count wire) max-rpc-size)
      (fail! :pubsub/rpc-too-large {:size (count wire) :limit max-rpc-size}))
    wire))

(defn decode-rpc [octets]
  (when (> (count octets) max-rpc-size)
    (fail! :pubsub/rpc-too-large {:size (count octets) :limit max-rpc-size}))
  (let [rpc (pb/decode rpc-schema octets)
        messages (mapv #(update % :data vec) (or (:messages rpc) []))]
    (when-not (every? strict-no-sign? messages)
      (fail! :pubsub/not-strict-no-sign {}))
    {:subscriptions (vec (or (:subscriptions rpc) []))
     :messages messages}))
