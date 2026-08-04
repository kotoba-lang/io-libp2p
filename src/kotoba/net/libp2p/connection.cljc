(ns kotoba.net.libp2p.connection
  "The thing that runs the libp2p layers in order over one socket.

  Every layer already existed here as a pure state machine with no socket:
  `libp2p.multistream`, `kotoba-lang/noise`, `libp2p.yamux`,
  `multiformats.multiaddr`. What was missing was their sequencing, and the
  sequencing is where libp2p is unforgiving:

      TCP → multistream(/noise) → Noise XX → multistream(/yamux) → Yamux
                                                                     ↓
                                       multistream(<protocol>) per stream

  Three framings stack, and each is different. Multistream frames are
  varint-length-prefixed strings ending in a newline that is *inside* the
  length. Noise handshake and transport messages are prefixed with two
  big-endian bytes. Yamux has a twelve-octet header whose `length` field means
  a byte count for DATA and a credit delta for WINDOW_UPDATE. Reading one
  layer's frame with another layer's rule desynchronizes the connection
  permanently and silently, because the bytes stay structurally valid.

  The **second multistream runs inside the encrypted channel**, which is easy to
  miss: after the Noise handshake completes, `/yamux/1.0.0` is negotiated as
  Noise transport messages, not as plaintext on the socket.

  The socket is injected as `{:read! (fn [n] octets) :write! (fn [octets])}`
  for the same reason the layers below hold no socket: a driver that opened its
  own connection could not be tested without a network, and this one is tested
  against an in-memory pair."
  (:require [clojure.string :as str]
            [kotoba.net.libp2p.handshake :as identity]
            [libp2p.multistream :as ms]
            [libp2p.yamux :as yamux]
            [noise.cipher-state :as cs]
            [noise.handshake-state :as noise]))

(def multistream-protocol "/multistream/1.0.0")
(def noise-protocol "/noise")
(def yamux-protocol "/yamux/1.0.0")

(def max-noise-message 65535)
(def max-multistream-message 1024)

(defn- fail! [problem data]
  (throw (ex-info (name problem) (assoc data :problem problem))))

;; ---------------------------------------------------------------------------
;; Framing

(defn- u16-be [n] [(bit-and (bit-shift-right n 8) 0xFF) (bit-and n 0xFF)])

(defn- read-u16-frame
  "Noise framing: two big-endian length octets, then that many."
  [{:keys [read!]}]
  (let [header (read! 2)
        length (+ (* 256 (nth header 0)) (nth header 1))]
    (when (zero? length) (fail! :libp2p/empty-noise-frame {}))
    (read! length)))

(defn- write-u16-frame [{:keys [write!]} octets]
  (when (> (count octets) max-noise-message)
    (fail! :libp2p/noise-frame-too-large {:size (count octets)}))
  (write! (into (u16-be (count octets)) octets)))

(defn- read-varint
  "Multistream framing is varint-prefixed, and the length must be read one
  octet at a time: reading ahead would consume the payload of the frame the
  length describes."
  [{:keys [read!]}]
  (loop [shift 0 acc 0 seen 0]
    (when (> seen 9) (fail! :libp2p/varint-too-long {}))
    (let [b (nth (read! 1) 0)
          acc (bit-or acc (bit-shift-left (bit-and b 0x7F) shift))]
      (if (zero? (bit-and b 0x80))
        acc
        (recur (+ shift 7) acc (inc seen))))))

(defn- read-multistream-message
  "One multistream frame, as the string it carries (newline stripped)."
  [port]
  (let [length (read-varint port)]
    (when (> length max-multistream-message)
      (fail! :libp2p/multistream-frame-too-large {:size length}))
    (let [octets ((:read! port) length)
          text (apply str (map char octets))]
      (if (str/ends-with? text "\n")
        (subs text 0 (dec (count text)))
        (fail! :libp2p/multistream-frame-unterminated {:text text})))))

(defn- write-multistream-message [port text]
  ((:write! port) (ms/encode text)))

(defn negotiate
  "Propose PROTOCOL over PORT and require the peer to echo it.

  The echo IS the acceptance -- there is no `ok` -- so anything else is a
  failure rather than something to interpret. The header and the first proposal
  go out together: one round trip per layer, and there are three."
  [port protocol]
  (write-multistream-message port multistream-protocol)
  (write-multistream-message port protocol)
  (let [header (read-multistream-message port)]
    (when-not (= multistream-protocol header)
      (fail! :libp2p/multistream-header-mismatch {:offered multistream-protocol :got header}))
    (let [reply (read-multistream-message port)]
      (when-not (= protocol reply)
        (fail! :libp2p/protocol-not-accepted {:wanted protocol :got reply}))
      protocol)))

;; ---------------------------------------------------------------------------
;; Secure channel

(defn- secure-port
  "A port that encrypts on the way out and decrypts on the way in.

  Buffered because the layers above read by byte count while Noise delivers
  whole messages: a caller asking for 12 octets of a Yamux header must not
  consume -- and lose -- the rest of the Noise message it arrived in."
  [port send-cs recv-cs]
  (let [send (volatile! send-cs)
        recv (volatile! recv-cs)
        buffer (volatile! [])]
    {:read!
     (fn [n]
       (while (< (count @buffer) n)
         (let [frame (read-u16-frame port)
               [next plain] (cs/decrypt-with-ad @recv [] frame)]
           (vreset! recv next)
           (vswap! buffer into plain)))
       (let [taken (vec (take n @buffer))]
         (vswap! buffer #(vec (drop n %)))
         taken))
     :write!
     (fn [octets]
       ;; Chunked to the Noise message limit; a single write above this layer
       ;; is not a single frame below it.
       (doseq [chunk (partition-all (- max-noise-message 16) octets)]
         (let [[next ct] (cs/encrypt-with-ad @send [] (vec chunk))]
           (vreset! send next)
           (write-u16-frame port ct))))}))

(defn handshake!
  "Run multistream(/noise) and the XX handshake, returning an encrypted port.

  Returns `{:port :peer}` where `:peer` carries the verified identity key. The
  remote static key comes from the handshake state, never from the payload --
  a payload that named its own key would prove nothing."
  [port {:keys [suite static identity-public-key sign-fn verify-fn prologue]}]
  (negotiate port noise-protocol)
  (let [payload (identity/payload {:identity-public-key identity-public-key
                                   :noise-static-public-key (:pub static)
                                   :sign-fn sign-fn})
        hs (noise/initialize {:suite suite :pattern :XX :initiator? true
                              :s static :prologue (or prologue [])})
        ;; XX message 1: e. No payload -- nothing is encrypted yet.
        [hs msg1] (noise/write-message hs [])
        _ (write-u16-frame port msg1)
        ;; XX message 2: e, ee, s, es -- and the responder's payload.
        [hs their-payload] (noise/read-message hs (read-u16-frame port))
        ;; XX message 3: s, se -- ours.
        [hs msg3] (noise/write-message hs payload)
        _ (write-u16-frame port msg3)]
    (when-not (:done? hs)
      (fail! :libp2p/handshake-incomplete {}))
    (let [verified (identity/verify their-payload (:rs hs) verify-fn)]
      (when-not (:ok? verified)
        (fail! :libp2p/peer-identity-unverified (dissoc verified :ok?)))
      {:port (secure-port port (:send-cs hs) (:recv-cs hs))
       :peer (assoc verified :noise-static-key (:rs hs))
       :handshake-hash (:handshake-hash hs)})))

;; ---------------------------------------------------------------------------
;; Muxed streams

(defn- write-frame [port frame] ((:write! port) frame))

(defn open!
  "Negotiate Yamux over the encrypted port and return a session handle."
  [secure]
  (negotiate secure yamux-protocol)
  (volatile! (yamux/session :dialer)))

(defn- read-yamux-frame [port]
  (let [header ((:read! port) 12)
        decoded (yamux/decode (vec header))
        length (:length decoded)]
    (if (= :data (:type decoded))
      (assoc decoded :payload (if (pos? length) (vec ((:read! port) length)) []))
      ;; A window update's `length` is a credit delta and nothing follows it.
      ;; Reading it as a byte count consumes the next header as payload and
      ;; desynchronizes the connection permanently, with no error.
      (assoc decoded :payload []))))

(defn stream!
  "Open a Yamux stream and negotiate PROTOCOL on it.

  Returns a port for the stream, so a caller speaks its protocol without
  knowing anything about frames."
  [secure session protocol]
  (let [{next-session :session stream-id :stream-id syn :out} (yamux/open-stream @session)
        _ (vreset! session next-session)
        ;; The SYN is a zero-length DATA frame; it opens the stream before any
        ;; payload rides it.
        _ (write-frame secure syn)
        inbox (volatile! [])
        port {:read! (fn [n]
                       (while (< (count @inbox) n)
                         (let [frame (read-yamux-frame secure)]
                           (when (= stream-id (:stream-id frame))
                             (vswap! inbox into (:payload frame)))))
                       (let [taken (vec (take n @inbox))]
                         (vswap! inbox #(vec (drop n %)))
                         taken))
              :write! (fn [octets]
                        (write-frame secure (yamux/data-frame stream-id #{} (vec octets))))}]
    (negotiate port protocol)
    (assoc port :stream-id stream-id)))
