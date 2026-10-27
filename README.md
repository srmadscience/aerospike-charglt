# aerospike-charglt

Aerospike implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt), but on Aerospike.

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

## Running

Start Aerospike in Docker:

    ./scripts/start-aerospike.sh

Build:

    mvn package

Then, using `target/aerospike-charglt-1.0-SNAPSHOT-jar-with-dependencies.jar` as the classpath:

| Class | Parameters |
|---|---|
| `ie.rolfe.aerospikecharglt.CreateChargingDemoData` | hostnames recordcount tpms maxinitialcredit |
| `ie.rolfe.aerospikecharglt.ChargingDemoTransactions` | hostnames recordcount tpms durationseconds queryseconds |
| `ie.rolfe.aerospikecharglt.ChargingDemoKVStore` | hostnames recordcount tpms durationseconds queryseconds jsonsize deltaProportion |
| `ie.rolfe.aerospikecharglt.DeleteChargingDemoData` | hostnames recordcount tpms |

e.g.

    J=target/aerospike-charglt-1.0-SNAPSHOT-jar-with-dependencies.jar
    java -cp $J ie.rolfe.aerospikecharglt.CreateChargingDemoData localhost 100000 200 1000
    java -cp $J ie.rolfe.aerospikecharglt.ChargingDemoTransactions localhost 100000 5 60 10
    java -cp $J ie.rolfe.aerospikecharglt.ChargingDemoKVStore localhost 100000 5 60 10 100 50
    java -cp $J ie.rolfe.aerospikecharglt.DeleteChargingDemoData localhost 100000 200

The benchmarks use the Aerospike async client. A single thread sends requests at `tpms`, and responses
arrive on one event loop per CPU, which update each user's client-side state. A user with a request
outstanding is skipped until it finishes, and at the end the benchmark waits for everything outstanding
to finish. Each event loop has at most `MAX_COMMANDS_PER_LOOP` requests on the wire, default 8, minimum 5.
Raising it past what the server can handle adds queueing latency without adding throughput.

`hostnames` is a comma delimited list of `host[:port]`; the port defaults to 3000. Set `AEROSPIKE_NAMESPACE`
to use a namespace other than `test`, and `EXTRA_MS` to slow things down on very slow hardware.

## See Also:

* [mongodb-charglt](https://github.com/srmadscience/mongodb-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [s2-charglt](https://github.com/srmadscience/s2-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)

## TODO

* Feed to downstream systems

## Status

While this is fine to play with, it's not a fair representation of Aerospike at the moment.
