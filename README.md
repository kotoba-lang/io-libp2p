# kotoba-net

**P2P content-distribution *semantics* (gossip/GossipSub-equivalent + bitswap-equivalent) in pure Clojure/EDN.**

This repo is the CLJC re-implementation of the semantic layer that used to
live in the (now-deleted, see `kotoba-lang/kotoba` PR #259 and
`docs/rust-crate-migration.md`) Rust crate `kotoba-net`, which combined
libp2p (QUIC transport + Noise handshake + GossipSub) with a bitswap-style
block exchange.

## Scope

**In scope — pure, transport-independent, deterministic functions:**

- `kotoba.net.gossip` — peer/topic bookkeeping, content-hash based message
  dedup via a bounded FIFO seen-cache, deterministic fanout peer selection,
  and `route-message` (dedup + fanout combined).
- `kotoba.net.bitswap` — want-list / have-list bookkeeping,
  want∩have intersection (`respond-to-want`), and the `WantSince` delta-sync
  request/response applied against a local commit-log (`commits-since`).

**Out of scope (left to a future native adapter):**

- Any real network I/O: QUIC/TCP/WebRTC datachannel transport, socket
  binding, connection lifecycle.
- Noise/TLS handshake, transport-level encryption, NAT traversal, peer
  discovery/DHT.
- Wire (de)serialization/framing of these EDN messages onto an actual byte
  stream — this repo defines the EDN shapes, not the bytes-on-wire.
- Tit-for-tat / bandwidth accounting, ledger/session bookkeeping for bitswap.

The premise (see `orgs/kotoba-lang/kotoba/docs/rust-crate-migration.md`): a
full libp2p-equivalent stack is not realistic to build from scratch in
Clojure/ClojureScript. What *is* tractable, and worth being the semantic
source of truth, is the protocol logic — fanout selection, dedup, want/have
resolution, delta-sync — as pure functions that any transport (JVM socket,
browser WebRTC, native Rust/Go adapter, etc.) can drive. See
`docs/ADR-kotoba-net-p2p-semantics.md` for the full rationale.

## Usage

```clojure
(require '[kotoba.net.gossip :as gossip])

(def state (-> (gossip/empty-peer-state)
               (gossip/add-peer "p1" #{"topic-a"})
               (gossip/add-peer "p2" #{"topic-a"})))

(def cache (gossip/empty-seen-cache 1000))

(gossip/route-message state cache
  {:topic "topic-a" :payload "hello" :from "p1" :self "me" :d 6})
;; => {:seen-cache {...} :forward [{:to "p2" :payload "hello"}]}
```

```clojure
(require '[kotoba.net.bitswap :as bitswap])

(bitswap/respond-to-want #{"cid-1" "cid-2"} #{"cid-2" "cid-3"})
;; => ["cid-2"]

(bitswap/commits-since commit-log (bitswap/make-want-since "graph-1" 1 "cid-head"))
;; => entries with seq > 1
```

## Test

```bash
clojure -M:test
```

## License

Apache-2.0.
