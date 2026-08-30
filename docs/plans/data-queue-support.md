# Implementation Plan: IBM i Data Queue Support

**Date:** 2026-08-30
**Status:** Implemented — see the [implementation addendum](#12-implementation-addendum) for verified protocol facts and deviations from this plan
**Repo:** `SharpSeries` — native C# IBM i driver

---

## 1. Goal

Add read, write, and peek support for IBM i data queues (`*DTAQ` objects) — both plain
(FIFO/LIFO) and keyed queues — over the native host-server protocol, in the same
spirit as the existing SQL support: no JT400 dependency, wire-level implementation
ported/inspired by JTOpen, exposed through a clean async-first C# API.

Today SharpSeries is SQL-only: it talks exclusively to the **as-database** host server
(QZDASOINIT) with server ID `0xE004` hardcoded in every packet
(`SharpSeries/HostServer/HostServerConnectionManager.cs:147,239,352,516`,
`SharpSeries/HostServer/QueryExecutor.cs:47,141,219,303,387`). There is no data-queue,
program-call, or `QCMDEXC` functionality anywhere in the codebase.

## 2. Non-goals (initial release)

- **TLS transport** — `HostServerStream.UpgradeToTlsAsync()` exists but is unused;
  wiring it up (port 9474 + certificate handling) is deferred.
- **Queue administration** — create/delete queues (`CRTDTAQ`/`DLTDTAQ`), clear queue.
  Users create queues out-of-band; a `DataQueueConnection` only uses existing queues.
- **Distributed/remote (`*DTAQ` over DDM), journaling, exit-program introspection.**
- **Events/notifications** (JT400's `DataQueueEvent` listening model).

## 3. Background

### 3.1 What we're talking to

| Fact | Value | Notes |
|---|---|---|
| Service name | `as-dtaq` | Registered with the server mapper on port 449 |
| Default port | **8474** (SSL 9474) | Fallback when the mapper is unreachable, mirroring the existing 8471 fallback |
| Server jobs | `QZHQSSRV` prestart jobs (QUSRWRK) | Started by `STRHOSTSVR *DTAQ` |
| Host-server ID | **`0xE007`** | JTOpen `AS400Server.getServerId()`: `as-dtaq` → 0xE007. (The 0xE005 figure from secondary sources is wrong — that is the database NDB server variant.) Verified against JTOpen source during implementation |
| Authentication | Same `0x7001` seed exchange + `0x7002` start-server challenge as the DB server | Already implemented and reusable verbatim |
| Entry limits | FIFO/LIFO and keyed entries up to ~64 KB (`CRTDTAQ MAXLEN ≤ 64512`); keyed key length 1–256 bytes | Confirm exact limits from CRTDTAQ docs; optionally enforce client-side after `GetAttributes` |

### 3.2 Model

- A queue entry is an **opaque byte string**; string convenience APIs encode/decode via
  the connection's CCSID (default 37, matching `Db2ConnectionStringBuilder.Ccsid`).
- Queues created with `SENDERINF(*YES)` return sender information with each entry
  (sending job name/user/number, current profile, timestamp) — parsed into a
  `DataQueueSenderInfo` object.
- `Read` is destructive; `Peek` is not. Both accept a **wait time**: `0` = no wait
  (return null if empty), `N > 0` = wait up to N seconds, `-1` = wait indefinitely.
- Keyed queues address entries by key with a search operator
  (EQ / GT / LT / GE / LE on unsigned byte comparison).

### 3.3 License

SharpSeries and JTOpen are both **IBM Public License 1.0**. Porting the DQ stream
code from `com.ibm.as400.access` is license-compatible. Every ported file carries the
same derivative-work header already used in
`SharpSeries/HostServer/HostServerConnectionManager.cs:1-5`.

## 4. Current state (what we build on)

| Component | File | Reuse for DTAQ |
|---|---|---|
| TCP + tracing | `SharpSeries/Network/HostServerStream.cs` | As-is |
| Mapper lookup | `HostServerConnectionManager.ResolveDatabasePortAsync` (line 105) | **Hardcoded as-database bytes (line 113) — must be generalized** |
| Sign-on flow | `ConnectAndAuthenticateAsync` (lines 44–100): 0x7001 seeds, DES/SHA-1 0x7002 | As-is (generic host-server sequence) |
| Reply framing | `ReceiveReplyWithBodyAsync` (line 277): length-prefixed packet reads | As-is |
| Envelope writer | `WriteDummyHostServerEnvelope` (line 347) | Parameterize server ID |
| LLCP block idiom | e.g. cursor-ID block `0x380B` in `CloseCursorAsync` (line 505) | Same building style for DQ structures |
| CCSID/EBCDIC | `SharpSeries/Encoding/CcsidConverter.cs` | As-is |
| Pool | `SharpSeries/Pool/ConnectionPool.cs` | **Statically typed to `HostServerConnectionManager` — must be generalized or duplicated** |
| Conn-string | `SharpSeries/Data/Db2ConnectionStringBuilder.cs` | Same keys reused (`Server`, `User ID`, `Password`, `CCSID`) |
| Logging | `SharpSeries/Logging/Db2Logger.cs` | As-is |
| Tests | `SharpSeries.Tests/` (converters, CCSID, pool) | Patterns to follow; `Tests/Drda/` is empty |

## 5. Design

Layering mirrors the existing split — wire protocol under `HostServer/`, public API
under a new `DataQueues/` namespace:

```
Network/HostServerStream.cs          (unchanged)
        │
HostServer/HostServerSessionBase.cs  (NEW, Phase 0 — extracted generic session)
        ├── HostServer/HostServerConnectionManager.cs   (SQL, becomes subclass)
        └── HostServer/DataQueueConnectionManager.cs    (NEW, Phase 1 — server ID 0xE005)
                    │
HostServer/DataQueueExecutor.cs      (NEW — DQ packet builders/parsers, mirrors QueryExecutor)
        │
DataQueues/  DataQueueConnection, DataQueue, KeyedDataQueue,
             DataQueueEntry, KeyedDataQueueEntry, DataQueueAttributes, ...
```

### Phase 0 — Extract a reusable host-server session (refactor, no behavior change)

1. Create `SharpSeries/HostServer/HostServerSessionBase` (abstract):
   - abstract `ushort ServerId { get; }` and abstract service identity
     (`ServiceName`, `FallbackPort`);
   - move the shared machinery out of `HostServerConnectionManager`:
     seed exchange (`SendRandomSeedsRequestAsync`), start-server challenge
     (`SendStartServerChallengeAsync`), `ReceiveReplyAsync` /
     `ReceiveReplyWithBodyAsync`, `Disconnect`, and the envelope writer with
     `ServerId` substituted for the hardcoded `0xE004`;
   - mapper lookup generalized to
     `ResolveServicePortAsync(host, serviceName, fallbackPort)` — build the request
     from `CcsidConverter.GetBytes(37, serviceName)` instead of the hardcoded
     15-byte array at `HostServerConnectionManager.cs:113`
     (those bytes are exactly `0x01 0x00 0x00 0x11` + EBCDIC `"as-database"`).
2. `HostServerConnectionManager : HostServerSessionBase` keeps all SQL methods and
   its public surface identical.
3. Golden-byte unit test: the generalized mapper builder must reproduce the current
   hardcoded `as-database` request byte-for-byte (proves no regression).

**Acceptance:** `dotnet test` green; `SampleIseriesReader`/`SampleIseriesWriter`
behave exactly as before.

### Phase 1 — FIFO/LIFO queues (core deliverable)

1. **`SharpSeries/HostServer/DataQueueConnectionManager.cs`**
   (`: HostServerSessionBase`): `ServerId = 0xE005`, `ServiceName = "as-dtaq"`,
   `FallbackPort = 8474`. No other connection logic needed — auth comes from the base.
2. **`SharpSeries/HostServer/DataQueueExecutor.cs`** — static packet
   writers/parsers in the `QueryExecutor` style:
   - `WriteEntryRequest` (queue name + library + optional sender info + data),
   - `ReadEntryRequest` (queue name + library + wait time + peek flag +
     request-sender-info flag),
   - `QueryAttributesRequest`,
   - reply parsers → entry length/data/sender info, attributes, or error
     (rc class / rc mapped to exceptions, mirroring the style at
     `HostServerConnectionManager.cs:416-425`).
   - Queue/library names: 10-char EBCDIC, space-padded, upper-cased — same
     convention as cursor names (`HostServerConnectionManager.cs:507`);
     `*LIBL` / `*CURLIB` supported for library.
   - Wait time encoding: `-1` → `0xFFFFFFFF`, else seconds — verify against JTOpen
     `DQReadRequest` during implementation.
3. **Public API in `SharpSeries/DataQueues/`:**
   - `DataQueueConnection` — connection string (reuses
     `Db2ConnectionStringBuilder` keys), `OpenAsync`/`Close`/`Dispose`,
     exposes `Ccsid`;
   - `DataQueue(connection, name, library = "*LIBL")`;
   - `DataQueueEntry` — `Data` (byte[]), `GetString(ccsid)`, `SenderInfo`
     (nullable), `SentTimestamp`;
   - `DataQueueSenderInfo` — job/user/number, current profile;
   - `DataQueueAttributes` — `MaxEntryLength`, `KeyLength`,
     `SenderInformationIncluded`, `QueueType` (FIFO/LIFO/Keyed);
   - `DataQueueException` — server rc + message.
4. **Pooling:** generalize `ConnectionPool` into
   `HostServerConnectionPool<TSession> where TSession : HostServerSessionBase`
   (or add a parallel static pool keyed `"dtaq|" + connectionString` — pick during
   implementation; the generic version is preferred). A **read-with-wait holds its
   session for the duration of the wait** — the connection is not in the pool while
   blocked. Document this.
5. Trace logging through `Db2Logger` — the existing hex-dump tracing in
   `HostServerStream` gives us free protocol debugging.

**Acceptance:** against a real system — write an entry, read it back, peek an entry
without consuming it, read attributes; no-wait read on empty queue returns null;
wait-read times out. Unit tests for every packet builder (golden hex) and reply
parser (synthetic arrays) pass.

### Phase 2 — Keyed queues

1. `KeyedDataQueue : DataQueue`, `KeyedDataQueueEntry` (adds `Key`),
   `KeySearchType` enum (`Equal`, `GreaterThan`, `LessThan`, `GreaterThanOrEqual`,
   `LessThanOrEqual`).
2. `DataQueueExecutor` additions: keyed write (key + data), keyed read/peek
   (key + search operator + wait).
3. Key handling: keys are raw bytes (1–256); string helpers encode via CCSID.
4. Tests: keyed golden packets, search-operator encoding, key parsing from replies.

**Acceptance:** write keyed entries, read back by exact and relative keys
(GT/LT/GE/LE), verify ordering semantics on a real system.

### Phase 3 — Lifecycle & pool hardening

1. Pool health-check on checkout (the TODO already noted at
   `ConnectionPool.cs:42`) — a lightweight request to validate the socket.
2. Optional `Max Pool Size` / `Connection Timeout` connection-string keys
   (both pool kinds).
3. Cancellation: `ReadAsync(wait: -1, ct)` should honor the cancellation token —
   in practice this means closing the socket; verify behavior and document it.

### Phase 4 — Samples, tests, docs

1. **Samples:** `SampleDataQueueWriter` / `SampleDataQueueReader` console projects
   mirroring the existing `SampleIseries*` projects; add to `SharpSeries.slnx`.
2. **Integration tests:** gated on environment variables
   (`SHARPSERIES_TEST_HOST`, `SHARPSERIES_TEST_USERID`, `SHARPSERIES_PASSWORD`);
   skipped when unset. Test fixture creates/destroys its queues via the **SQL side
   of the driver** (`Db2Command` calling `QSYS2.QCMDEXC('CRTDTAQ ...')`), which
   conveniently dogfoods both halves.
3. **Docs:** new "Data Queues" chapter in `USERGUIDE.md`; feature list + usage
   snippet in `README.md`; derivative-work headers on all ported files.
4. **License hygiene:** confirm each ported file lists JTOpen file(s) of origin.

## 6. Public API sketch

```csharp
using var conn = new DataQueueConnection("Server=myhost;User ID=MYUSER;Password=secret;");
await conn.OpenAsync(ct);

var queue = new DataQueue(conn, "ORDERQ", library: "APPLIB");

// Write (string encoded with the connection CCSID, or raw bytes)
await queue.WriteAsync("Hello from SharpSeries", ct);
await queue.WriteAsync(Encoding.UTF8.GetBytes(payload), ct);

// Read: 0 = no wait, N = wait N seconds, -1 = wait forever
DataQueueEntry? entry = await queue.ReadAsync(waitSeconds: 30, ct);
if (entry is not null)
{
    string text = entry.GetString(conn.Ccsid);
    DataQueueSenderInfo? sender = entry.SenderInfo;  // null unless SENDERINF(*YES)
}

// Peek (non-destructive)
DataQueueEntry? peeked = await queue.PeekAsync(ct);

// Keyed queues (Phase 2)
var keyed = new KeyedDataQueue(conn, "ORDERQ", "APPLIB");
await keyed.WriteAsync(key: "CUST0042", data: payloadBytes, ct);
KeyedDataQueueEntry? e = await keyed.ReadAsync("CUST0042", KeySearchType.Equal, waitSeconds: 10, ct);

// Attributes
DataQueueAttributes attrs = await queue.GetAttributesAsync(ct);
```

## 7. Wire protocol reference — where the truth lives

**Do not guess bytes.** The authoritative sources, in order:

1. **JTOpen source** (`https://github.com/IBM/JTOpen`,
   `src/main/java/com/ibm/as400/access/`):
   - Public model: `DataQueue.java`, `KeyedDataQueue.java`, `DataQueueEntry.java`,
     `KeyedDataQueueEntry.java`, `DataQueueAttributes.java`.
   - Wire streams: the `DQ*Request` / `DQ*Reply` classes (write, read, keyed
     variants, query-attributes). Port their layouts and constants directly.
   - `ServiceConstants.java` — confirm `SERVICE_DTAQ = 0xE005`.
   - `PortMapper.java` — confirm the mapper request framing for `as-dtaq`.
2. **Reference traces:** run JT400 against a real system with data-stream tracing
   enabled (`-Dcom.ibm.as400.access.Trace=true`) and capture hex dumps of
   write/read/peek/keyed operations. These become the **golden fixtures** for the
   unit tests in Phase 1/2.
3. **IBM documentation** — host-server ports/mapper, data queue server exit points
   (`QIBM_QZHQ_DATA_QUEUE`), `CRTDTAQ` limits, "Using data queues" in the
   Programming category.

## 8. Testing strategy

| Level | What | How |
|---|---|---|
| Unit — golden bytes | Every request builder produces exact expected hex | Fixtures captured from JT400 traces (Phase 1 task) |
| Unit — parsers | Reply parsing: entry/sender info/attributes/error rc | Hand-built byte arrays, edge cases (empty queue, no sender info, max-length entry) |
| Unit — encoding | Name padding/upper-casing, `*LIBL`/`*CURLIB`, wait-time encoding | Follow `CcsidConverterTests` style |
| Integration | Round-trips against a live system; FIFO/LIFO/keyed; wait semantics | Env-var-gated; fixtures create queues via `QSYS2.QCMDEXC` through `Db2Connection` |

## 9. Risks & open questions

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Exact DQ request/reply layouts are not verified in this plan** (biggest risk) | Port from JTOpen source; validate every packet against JT400 traces before writing parsers |
| 2 | Server ID `0xE005` / service name `as-dtaq` from secondary sources | Confirm in `ServiceConstants.java` / `PortMapper.java` — one-line check, Phase 1 step 0 |
| 3 | Error-code mapping (queue missing, authority, entry too large) unknown territory | Start with generic `DataQueueException(rc)`; refine codes as observed |
| 4 | Blocking reads pin pooled sessions; many waiters = many sockets | Document; pool sizing keys in Phase 3 |
| 5 | Sender-info layout/CCSID variations | Port `DataQueueEntry` sender parsing from JTOpen; fixture-test |
| 6 | Phase 0 refactor regresses SQL path | Golden mapper-bytes test + existing suite + samples as smoke test |
| 7 | DES (PWDLVL ≤ 1) path on the DTAQ server | Same generic sign-on code as DB; low risk, note in integration matrix |

## 10. Milestones

| Phase | Deliverable | Rough effort |
|---|---|---|
| 0 | Shared session base, generalized mapper, no behavior change | ~0.5–1 day |
| 1 | FIFO write/read/peek/attributes + pooling + golden tests | 2–4 days (incl. protocol verification) |
| 2 | Keyed queues | 1–2 days |
| 3 | Pool hardening, cancellation | 1 day |
| 4 | Samples, integration harness, docs | 1 day |

Phases 0→1 are strictly sequential; 2 depends on 1; 3 and 4 can interleave with 2.

## 11. References

- IBM i 7.5 — Port numbers for host servers and server mapper:
  https://www.ibm.com/docs/en/i/7.5.0?topic=numbers-port-host-servers-server-mapper
- IBM i — Server table (QZHQSSRV / QUSRWRK):
  https://www.ibm.com/docs/en/ssw_ibm_i_75/rzaku/rzakuservertable.htm
- IBM i — Using data queues:
  https://www.ibm.com/docs/en/i/7.4.0?topic=procedures-using-data-queues
- IBM i — Data queue server exit point (QIBM_QZHQ_DATA_QUEUE):
  https://www.ibm.com/docs/en/i/7.6.0?topic=parameters-data-queue-server
- JTOpen source: https://github.com/IBM/JTOpen
- JT400 `DataQueue` javadoc:
  https://javadoc.io/doc/net.sf.jt400/jt400-jdk6/9.8/com/ibm/as400/access/DataQueue.html

## 12. Implementation addendum

Implemented 2026-08-30. All wire layouts were ported directly from the JTOpen source
files listed in section 7 rather than derived from this plan's assumptions. Facts that
changed (or were confirmed) during implementation:

- **Server ID is `0xE007`, not `0xE005`.** JTOpen's `AS400Server.getServerId()` maps
  `as-dtaq` → `0xE007` (0xE005 is the as-database *NDB* variant). The plan's original
  figure came from an unreliable secondary source.
- **Keyed search supports six operators, not five**: EQ, NE, LT, LE, GT, GE, each sent
  as a 2-byte EBCDIC operand in the read template.
- **Sender information has no timestamp.** The 36-byte block is job name (10) +
  user name (10) + job number (6) + current user profile (10); an all-spaces block
  means the queue does not save sender info. The planned `SentTimestamp` was dropped.
- **The Server Mapper request is the plain ASCII service name** (no header), and the
  reply is `+` followed by a 4-byte big-endian port (JTOpen `AS400PortMapDS` /
  JTOpenLite `PortMapper`). The pre-existing `ResolveDatabasePortAsync` did not speak
  this protocol correctly — it sent an EBCDIC name with a bogus header and parsed the
  reply as text, so it silently always fell back to the hardcoded port (8471, which
  masked the bug). The generalized `ResolveServicePortAsync` fixes this for all
  services; the fallback remains for unreachable mappers.
- **Fallback port discrepancy:** JTOpen's `PortMapper` default table lists 8472 for the
  data queue server, contradicting IBM's host-server port documentation (as-dtaq =
  8474). We follow IBM's documentation; the mapper resolves the real port at runtime
  either way.
- **A mandatory exchange-attributes handshake** (request 0x0000, declaring client
  version 1 = 64K entry support; normal reply 0x8000) must be the first data request
  on a fresh data queue server connection — done automatically by
  `DataQueueConnectionManager.ConnectAndAuthenticateAsync`.
- **Reply codes:** success replies carry rc `0xF000`; an empty/matching-nothing read
  returns rc `0xF006` (mapped to a null result); CPF message IDs ride the common
  reply's message block and map to specific exceptions.

Delivered: shared `HostServerSessionBase` (Phase 0), FIFO/LIFO write/read/peek plus
attributes (Phase 1), keyed queues (Phase 2), generic session pooling, 58 unit tests
including byte-level golden packet tests, sample applications
(`SampleDataQueueWriter`/`SampleDataQueueReader`), and documentation updates.
Phase 3's pool health-check and max-pool-size knobs remain future work (noted as a
TODO in the pool). Live-system validation against a real IBM i is still pending.
