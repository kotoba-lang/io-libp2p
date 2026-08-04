(ns kotoba.net.libp2p.serve
  "Answering, rather than only asking.

  A client dials, identifies and queries; every inbound stream it resets. That
  takes from the network without serving it, and for a DHT the asymmetry is not
  merely impolite: a node nobody can query holds no useful position in the
  keyspace, so its own lookups get worse answers than a serving peer's.

  This namespace is the protocol half of serving -- what to reply, given a
  request and a routing table. It opens no socket and holds no connection, so
  the answers are testable without a peer, which matters because the failure
  mode of a wrong answer is a peer quietly deciding you are useless rather than
  an error anyone sees."
  (:require [kad.message :as kad]
            [multiformats.multiaddr :as multiaddr]
            [kotoba.net.libp2p.handshake :as identity]
            [kotoba.net.libp2p.identify :as identify]
            [protobuf.wire :as pb]))

(def supported-protocols
  "What this node answers. Advertised in identify and accepted by the listener;
  the two must agree, because a peer that believes an advertisement and gets
  `na` has been lied to."
  [identify/protocol "/ipfs/kad/1.0.0"])

(defn identify-response
  "Our own identify snapshot.

  `listen-addrs` are what we believe we listen on -- a claim, exactly as it is
  when a peer sends it to us, and labelled as one everywhere it is read.

  They go on the wire as multiaddr OCTETS. Passing the human string is the
  mistake that is easy to make and hard to see: protobuf will happily length-
  prefix a string into a `bytes` field, the message parses on our side, and the
  peer reports only `error reading identify message` -- which is what go-libp2p
  said about this exact bug."
  [{:keys [identity-public-key listen-addrs observed-addr agent-version]
    :or {agent-version "kotoba-libp2p/0.1"}}]
  (pb/encode identify/schema
             (cond-> {:public-key (identity/public-key-protobuf identity-public-key)
                      :protocols (vec supported-protocols)
                      :protocol-version "ipfs/0.1.0"
                      :agent-version agent-version
                      :listen-addrs (mapv #(if (string? %) (multiaddr/->octets %) (vec %))
                                          (or listen-addrs []))}
               observed-addr (assoc :observed-addr
                                    (if (string? observed-addr)
                                      (multiaddr/->octets observed-addr)
                                      (vec observed-addr))))))

(defn find-node-response
  "Answer FIND_NODE with the closest peers we know.

  `closest` is supplied by the caller from its routing table rather than
  computed here: which peers are closest is a fact about the table's contents
  and the XOR metric, and `kad.table` already owns both."
  [request closest]
  (kad/encode {:type (:type request)
               :key (:key request)
               :closer-peers (vec closest)}))

(defn respond
  "The reply to one decoded kad request, or nil when we have nothing to say.

  PUT_VALUE and ADD_PROVIDER are accepted and not stored: storing records for
  other people is a commitment to keep and re-serve them, and claiming it
  without a store behind it would make this node a black hole that advertises
  itself as a replica."
  [request {:keys [closest]}]
  (let [type (:type request)]
    (condp = type
      (kad/message-type :find-node) (find-node-response request (closest (:key request)))
      (kad/message-type :get-providers) (find-node-response request (closest (:key request)))
      (kad/message-type :get-value) (find-node-response request (closest (:key request)))
      ;; A ping needs no body beyond the echo.
      (kad/message-type :ping) (kad/encode {:type type})
      nil)))
