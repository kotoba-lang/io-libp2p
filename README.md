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

**Real I/O, on top of `kotoba-lang/wire` — `kotoba.net.transport.tcp`:**

- A plain-TCP "gossip node" (`src/kotoba/net/transport/tcp.cljs`,
  Node-only/nbb) that actually moves the `:kotoba.net.gossip`/
  `:kotoba.net.bitswap` protocol messages above between real peers over
  real sockets, delegating all framing/socket-pool mechanics to
  [`kotoba-lang/wire`](https://github.com/kotoba-lang/wire) — the same
  dependency [`kotoba-lang/dtn`](https://github.com/kotoba-lang/dtn)'s own
  TCP transport already runs on. See "Real TCP transport" below.

**Out of scope (left to a future adapter):**

- QUIC/WebRTC datachannel transport (`kotoba.net.transport.tcp` is plain
  TCP only — see below).
- Noise/TLS handshake, transport-level encryption, NAT traversal, peer
  discovery/DHT (a node's peers are configured up front; it never learns
  about a peer it wasn't told about — the same limitation
  `kotoba-lang/dtn`'s own direct transport has).
- Tit-for-tat / bandwidth accounting, ledger/session bookkeeping for bitswap.

The premise (see `orgs/kotoba-lang/kotoba/docs/rust-crate-migration.md`): a
full libp2p-equivalent stack is not realistic to build from scratch in
Clojure/ClojureScript. What *is* tractable, and worth being the semantic
source of truth, is the protocol logic — fanout selection, dedup, want/have
resolution, delta-sync — as pure functions that any transport (JVM socket,
browser WebRTC, native Rust/Go adapter, etc.) can drive. See
`docs/ADR-kotoba-net-p2p-semantics.md` for the full rationale.

## Maturity

| | |
|---|---|
| Role | capability |
| Tests | 53 assertions, all green (`clojure -M:test`, pure `.cljc` only) |
| Real TCP transport (`kotoba.net.transport.tcp`) | yes — plain TCP via nbb, built on `kotoba-lang/wire`; E2E demo green (3/3 scenarios), see below |
| Gossip fanout + dedup over real sockets | yes — real multi-node mesh delivery, seen-cache dedup proven to suppress redundant re-delivery across real OS processes |
| Bitswap want/have over real sockets | yes — real request/response round-trip, matches pure `respond-to-want` |
| Bitswap delta-sync over real sockets | yes — real request/response round-trip, matches pure `commits-since` |
| Peer discovery / DHT | no (peers configured up front only — see Scope) |
| NAT traversal / QUIC / Noise | no (plain TCP, cleartext, unauthenticated at the transport level — see Scope) |

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

## Real TCP transport (`kotoba.net.transport.tcp`)

Everything above is intentionally I/O-free. `kotoba.net.transport.tcp`
(`src/kotoba/net/transport/tcp.cljs`) is the real network-I/O adapter this
repo's own founding scope explicitly deferred: a "gossip node" that
actually moves `kotoba.net.gossip`/`kotoba.net.bitswap` protocol messages
between real peers over real TCP sockets. It's `.cljs`, not `.cljc` — it
only runs under a Node-hosted ClojureScript runtime
([`nbb`](https://github.com/babashka/nbb) in this repo) and is never
loaded by the JVM `clojure -M:test` suite, so it cannot regress the 53
pure-data assertions above.

**Wire framing and socket-pool plumbing come from
[`kotoba-lang/wire`](https://github.com/kotoba-lang/wire)** — the same
shared library [`kotoba-lang/dtn`](https://github.com/kotoba-lang/dtn)'s
own TCP transport already runs on (`kotoba.wire.tcp/start-server!` /
`connect-or-reuse!` / `send-framed!` / `close-all!`). This namespace never
hand-rolls length-prefix framing, buffer accumulation/defragmentation, or
an outbound socket pool — the entire point of depending on `wire` instead
of re-implementing that plumbing a second time.

```clojure
(require '[kotoba.net.transport.tcp :as tcp])

;; Node A: knows about B and C, both subscribed to "topic-a"
(def node-a
  (tcp/start-node! {:node-id "a" :port 5300
                     :peers {"b" {:host "127.0.0.1" :port 5301 :topics #{"topic-a"}}
                             "c" {:host "127.0.0.1" :port 5302 :topics #{"topic-a"}}}}))

;; Real fanout over real sockets -- returns the peers actually written to
(tcp/publish! node-a "topic-a" "hello")
;; => [{:to "b" :payload "hello"} {:to "c" :payload "hello"}]

;; Bitswap want/have and delta-sync round-trip over a real socket
(tcp/want! node-a "b" #{"cid-1" "cid-2"})                              ;; => Promise<vector>
(tcp/request-commits-since! node-a "b" (bitswap/make-want-since "g" 1 "head"))  ;; => Promise<vector>

(tcp/stop-node! node-a)
```

`start-node!` accepts `:have-set` (a `#{cid ...}` this node responds to
`:bitswap-want` requests from, since this transport layer has no idea what
content a real host actually holds — that's host-injected) and
`:commit-log` (a `[{:seq n :cid c} ...]` vector this node answers
`:bitswap-commits-since` delta-sync requests from, same reasoning). See the
namespace docstring for the full contract, including two real gaps
discovered in `kotoba.net.gossip` while wiring this up — neither fixed by
editing `gossip.cljc` (out of scope for this transport work; worked around
entirely from `kotoba.net.transport.tcp` instead):

1. `content-hash`'s `:cljs` branch calls a real Google Closure Library
   class (`goog.crypt.Sha256`) that a full Closure-Compiler build
   (shadow-cljs) provides for free but plain `nbb` does not bundle.
   `src/goog/crypt/Sha256.cljs` is an nbb-classpath-only shim (Node's
   built-in `node:crypto`, no npm dependency — the same pattern
   `kotoba-lang/dtn`'s `kotoba.dtn.auth` already uses for its own
   HMAC-SHA256 under nbb) that supplies exactly the two methods
   `content-hash` calls, verified against known SHA-256 test vectors.
   `clojure -M:test` and a shadow-cljs build are unaffected — this shim is
   never on either of those classpaths.
2. `route-message`/`gossip-fanout` build their exclude set via the literal
   `#{from self}` syntax, which throws `Duplicate key` (real Clojure/
   ClojureScript/SCI behavior, reproduced identically under plain JVM
   `clojure -M -e`, not an nbb-only quirk) whenever `:from` and `:self`
   evaluate to the same value — exactly what a locally-originated
   `publish!` naturally produces (there's no previous hop to exclude, only
   self). `kotoba.net.transport.tcp` never hands `route-message` a
   colliding pair (see `safe-from` in the namespace source).

### E2E demo (`test/kotoba/net/transport/tcp_demo.cljs`)

An executable proof, not a unit test — run it and read the output:

```bash
nbb --classpath "src:test:../wire/src:../bytes/src" \
  test/kotoba/net/transport/tcp_demo.cljs
```

(`--classpath` mirrors `deps.edn`'s sibling `:local/root` layout — this
repo checked out next to `kotoba-lang/wire` and `kotoba-lang/bytes`.)

Three scenarios, printing `PASS`/`FAIL` per scenario and a final
`RESULT: N/3 scenarios passed` line (exit 0 iff 3/3):

1. **3-node gossip fanout + dedup, real TCP.** Node A (in-process) knows
   B and C; B and C also know each other (a full mesh, not just A→B) and
   both are spawned as real, separate `nbb` OS processes via
   `bin/net_node.cljs listen` — mirroring `kotoba-lang/dtn`'s own
   strongest-form scenario 1 (spawn a real child process, verify via its
   own stdout, not just a local return value). A `publish!`s one message;
   the demo confirms both B and C actually receive it (grepping each
   child's own stdout for its `NET-GOSSIP-RECV` log line) — and, because B
   and C also gossip to each other, each node genuinely receives a SECOND,
   redundant copy over the wire via the other's re-forward. The demo
   confirms each node's receipt count stays at exactly 1 (not 2), proving
   `route-message`'s seen-cache dedup is real, not just that no redundant
   copy happened to be sent (verified with a negative-control run — the
   dedup-gate log line temporarily forced to always fire — before this
   demo was finalized: the assertion correctly flipped to FAIL with a
   receipt count of 2, confirming it isn't vacuously true).
2. **Bitswap want/have, real TCP.** Node A has a `:have-set`; node B
   `want!`s a set including a CID A doesn't have. The demo confirms B's
   real `:bitswap-have` response over the wire is exactly the intersection
   pure `bitswap/respond-to-want` would compute — the missing CID is
   genuinely excluded, not just locally assumed.
3. **Bitswap delta-sync, real TCP.** Node A has a `:commit-log`; node B
   `request-commits-since!`s from a known sequence point. The demo
   confirms B's real `:bitswap-commits` response is exactly what pure
   `bitswap/commits-since` would compute.

### CLI (`bin/net_node.cljs`)

A minimal demo/dev tool — no config file, no auth, no encryption — used by
the E2E demo above to spawn real listening nodes as separate OS processes:

```bash
nbb --classpath "src:../wire/src:../bytes/src" bin/net_node.cljs listen \
  --node-id b --port 5301 \
  --peer a:127.0.0.1:5300:topic-a --peer c:127.0.0.1:5302:topic-a
```

## Test

```bash
clojure -M:test    # pure .cljc — kotoba.net.gossip, kotoba.net.bitswap,
                    # kotoba.net.transport.envelope
clojure -M:lint     # clj-kondo, 0 errors
```

`kotoba.net.transport.tcp` (real socket I/O) is `.cljs`-only and is
exercised by the E2E demo above rather than having its own JVM-runnable
test suite — the same split `kotoba-lang/dtn`'s transport uses. Its
peer-id-resolution and envelope-construction logic is pure enough to
factor out (`kotoba.net.transport.envelope`) and IS covered by
`clojure -M:test`.

## License

Apache-2.0.
