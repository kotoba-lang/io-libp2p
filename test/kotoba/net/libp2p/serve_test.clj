(ns kotoba.net.libp2p.serve-test
  "Answering: what a node replies, and the wire-shape mistake that made both
  of its replies unreadable."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [ed25519.core :as ed]
            [kad.message :as kad]
            [kotoba.net.libp2p.dnsaddr :as dnsaddr]
            [kotoba.net.libp2p.identify :as identify]
            [kotoba.net.libp2p.serve :as serve]
            [multiformats.multiaddr :as multiaddr]))

(defn- seed [n] (byte-array (map unchecked-byte (repeat 32 n))))

(deftest an-identify-response-round-trips-and-advertises-what-we-answer
  (let [octets (serve/identify-response
                {:identity-public-key (ed/pubkey-from-seed (seed 1))
                 :listen-addrs ["/ip4/127.0.0.1/tcp/45021"]})
        parsed (identify/parse octets)]
    (is (= "kotoba-libp2p/0.1" (:agent-version parsed)))
    (is (= (vec serve/supported-protocols) (:protocols parsed))
        "what we advertise must be what the listener accepts; a peer that
         believes an advertisement and gets `na` has been lied to")))

(deftest listen-addrs-go-on-the-wire-as-multiaddr-octets
  ;; The regression, twice over. protobuf length-prefixes a string into a
  ;; `bytes` field without complaint, so the message encodes, parses on our
  ;; side, and go-libp2p reports only `error reading identify message`.
  (let [address "/ip4/127.0.0.1/tcp/45021"
        parsed (identify/parse
                (serve/identify-response
                 {:identity-public-key (ed/pubkey-from-seed (seed 1))
                  :listen-addrs [address]}))]
    (is (= [(vec (multiaddr/->octets address))]
           (mapv vec (:listen-addrs parsed))))
    (is (= address (multiaddr/->string (vec (first (:listen-addrs parsed))))))))

(deftest find-node-is-answered-with-the-peers-we-were-given
  (let [closest [{:id [1 2 3] :addrs [(vec (multiaddr/->octets "/ip4/1.2.3.4/tcp/4001"))]
                  :connection 0}]
        request (kad/find-node [9 9 9])
        reply (kad/decode (serve/respond request {:closest (constantly closest)}))]
    (is (= (kad/message-type :find-node) (:type reply)))
    (is (= 1 (count (:closer-peers reply))))
    (is (= [1 2 3] (vec (:id (first (:closer-peers reply))))))))

(deftest a-request-we-do-not-serve-produces-no-reply
  ;; PUT_VALUE is accepted and not stored. Answering as though it were stored
  ;; would make this node advertise itself as a replica it is not.
  (is (nil? (serve/respond {:type (kad/message-type :put-value) :key [1]}
                           {:closest (constantly [])}))))

(deftest a-dnsaddr-is-filtered-to-the-peer-it-named
  ;; A host may answer for several peers. Taking the first record because it
  ;; parsed is how a dialer authenticates a peer it did not mean to reach --
  ;; and that peer verifies correctly, being simply the wrong node.
  (with-redefs [dnsaddr/resolve-address
                (fn [_] ["/ip4/1.1.1.1/tcp/4001/p2p/12D3KooWWanted"
                         "/ip4/2.2.2.2/tcp/4001/p2p/12D3KooWOther"])]
    (is (= 2 (count (dnsaddr/resolve-dialable "/dnsaddr/example" (constantly true)))))
    (is (= 1 (count (dnsaddr/resolve-dialable "/dnsaddr/example"
                                              #(str/includes? % "1.1.1.1")))))))

(deftest dnsaddr-recognises-only-what-it-can-resolve
  (is (true? (dnsaddr/dnsaddr? "/dnsaddr/bootstrap.libp2p.io")))
  (is (false? (dnsaddr/dnsaddr? "/ip4/1.2.3.4/tcp/4001")))
  (is (nil? (dnsaddr/resolve-address "/ip4/1.2.3.4/tcp/4001"))))
