# aerospike-charglt

Aerospike implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt), but on Aerospike.

Charglt simulates a telco charging system. Users have a balance, sessions reserve credit against it, and
the client repeatedly reports usage and asks for more ("Report Usage and Reserve More") or tops up a user
whose balance is running low ("Add Credit"). Every transaction has to be atomic and idempotent. There's also
a key-value benchmark that soft-locks a user, then updates their data and releases the lock.

It has the basic functionality of charglt, but is missing stuff - see TODO. It's derived from
[redis-charglt](https://github.com/srmadscience/redis-charglt).

## How it uses Aerospike

Each user is one record in `test.charglt_users`, keyed by user id:

| Bin | Type | Contents |
|---|---|---|
| `userId` | integer | user id |
| `lastSeen` | integer | epoch ms |
| `lockSessionId` | integer | soft lock owner, `Long.MIN_VALUE` if unlocked |
| `lockExpiry` | integer | epoch ms, 0 if unlocked |
| `balance` | integer | current balance |
| `reserved` | integer | total credit currently reserved by sessions |
| `userData` | map | loyalty scheme name / number and an arbitrary hex payload |
| `usage` | string | JSON of reservations by session |
| `txns` | string | JSON of recent transactions, used for idempotency |

* **Transactions** (`AddCredit`, `ReportQuotaUsage`) do an async read of the record, apply the business
  logic in `UserTable`, and do an async write back with a generation check (`EXPECT_GEN_EQUAL`), retrying
  if someone else changed the record in the meantime.
* **KV store** lock and update are each a single `operate()` call with a filter expression, so the
  "is it locked?" check and the write are atomic. Delta updates change one key in the `userData` map
  server side instead of sending the whole payload.
* **Running totals** are a set query that only returns the `reserved` bin.
* **Startup cleanup** (stale locks from `ChargingDemoKVStore`, stale reservations from
  `ChargingDemoTransactions`) is a query filtered server side to just the affected users, followed by
  batch writes.

### Async client

All four programs use the Aerospike async client. A single thread sends requests at `tpms`, and responses
arrive on one event loop per CPU, which update each user's client-side state. A user with a request
outstanding is skipped until it finishes, and at the end each program waits for everything outstanding
to finish.

Each event loop has at most `MAX_COMMANDS_PER_LOOP` requests on the wire (default 8, minimum 5).
Raising it past what the server can handle adds queueing latency without adding throughput - on Docker
Desktop, going from 8 to 100 left throughput unchanged and took 95th percentile latency from ~10ms to ~100ms.
A real cluster can probably use a higher value.

## Running

### Prerequisites

* Java 21+
* Maven
* Docker, or an existing Aerospike cluster

### Start Aerospike

    ./scripts/start-aerospike.sh

This creates a single node Aerospike Community Edition container called `aerospike` listening on
`localhost:3000`, using the `test` namespace from the image's default config. After that, use
`docker stop aerospike` and `docker start aerospike`. The `test` namespace is in memory, so data is lost
when the container stops - run `CreateChargingDemoData` again after a restart.

### Build

    mvn package

This runs the unit tests and builds `target/aerospike-charglt-1.0-SNAPSHOT-jar-with-dependencies.jar`.

### Run

| Class | Parameters |
|---|---|
| `ie.rolfe.aerospikecharglt.CreateChargingDemoData` | hostnames recordcount tpms maxinitialcredit |
| `ie.rolfe.aerospikecharglt.ChargingDemoTransactions` | hostnames recordcount tpms durationseconds queryseconds |
| `ie.rolfe.aerospikecharglt.ChargingDemoKVStore` | hostnames recordcount tpms durationseconds queryseconds jsonsize deltaProportion |
| `ie.rolfe.aerospikecharglt.DeleteChargingDemoData` | hostnames recordcount tpms |

| Parameter | Purpose | Example |
|---|---|---|
| hostnames | Comma delimited list of `host[:port]`. The port defaults to 3000. | localhost |
| recordcount | How many users. Use the same value for every program. | 100000 |
| tpms | Target transactions per millisecond. | 10 |
| maxinitialcredit | Users start with a random credit up to this. | 1000 |
| durationseconds | How long the benchmark runs for. | 60 |
| queryseconds | How often to show one user and the total reserved credit across all users. | 10 |
| jsonsize | Length of the arbitrary hex payload sent by a full update. | 100 |
| deltaProportion | Percentage of KV updates that change only the loyalty number instead of sending the whole payload. | 50 |

e.g.

    J=target/aerospike-charglt-1.0-SNAPSHOT-jar-with-dependencies.jar
    java -cp $J ie.rolfe.aerospikecharglt.CreateChargingDemoData localhost 100000 200 1000
    java -cp $J ie.rolfe.aerospikecharglt.ChargingDemoTransactions localhost 100000 10 60 10
    java -cp $J ie.rolfe.aerospikecharglt.ChargingDemoKVStore localhost 100000 10 60 10 100 50
    java -cp $J ie.rolfe.aerospikecharglt.DeleteChargingDemoData localhost 100000 200

The benchmarks end with a `GREPABLE SUMMARY:` line containing the target tpms, the achieved TPS and latency
percentiles, and exit with status 1 and `UNABLE_TO_MEET_REQUESTED_TPS` if they got less than 90% of the
requested rate.

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `AEROSPIKE_NAMESPACE` | Namespace to use | `test` |
| `MAX_COMMANDS_PER_LOOP` | Max requests on the wire per event loop, minimum 5 | 8 |
| `EXTRA_MS` | Extra sleep in ms each time a millisecond's quota of `tpms` is used up, for very slow hardware | 0 |

## Sample results

200,000 users, target 100 tpms, one Aerospike CE node in Docker Desktop on an 8 core Mac, with the client
on the same machine:

| Program | Achieved |
|---|---|
| `ChargingDemoTransactions` | ~17,000 TPS, 95th percentile ~10ms |
| `ChargingDemoKVStore` | ~30,000 TPS |
| `CreateChargingDemoData` | ~30 users per ms |
| `DeleteChargingDemoData` | ~30 users per ms |

The limit here is the single node and Docker Desktop's networking, not the client. Each transaction is
two round trips (read, then a generation-checked write).

## See Also:

* [mongodb-charglt](https://github.com/srmadscience/mongodb-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [s2-charglt](https://github.com/srmadscience/s2-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)

## TODO

* Feed to downstream systems
* Multi-node cluster results

## Status

While this is fine to play with, it's not a fair representation of Aerospike at the moment - it's only
been run against a single node in Docker Desktop.
