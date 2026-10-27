/*
 * Copyright (C) 2026 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.aerospikecharglt;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.google.gson.Gson;
import ie.rolfe.aerospikecharglt.documents.ExtraUserData;
import ie.rolfe.aerospikecharglt.documents.UserTable;
import ie.rolfe.aerospikecharglt.documents.UserUsageTable;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 * <p>
 * Benchmarks run on a single thread that sends requests using the Aerospike async client.
 * Responses arrive on the client's event loop threads, which update the per-user state objects.
 */
public abstract class BaseChargingDemo {

    public static final long GENERIC_QUERY_USER_ID = 42;
    public static final long NO_SESSION = Long.MIN_VALUE;
    public static final Date NO_EXPIRY = new Date(0);

    public static final String REPORT_QUOTA_USAGE = "ReportQuotaUsage";
    public static final String KV_PUT = "KV_PUT";
    public static final String KV_GET = "KV_GET";
    public static final String DELETE_DOC = "Delete Doc";
    public static final String DELETE_DOC_ERROR = "Delete Doc Error";
    public static final String ADD_DOC = "Add Doc";
    public static final String ADD_DOC_ERROR = "Add Doc Error";
    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";
    public static final String EXTRA_MS = "EXTRA_MS";
    public static final String AEROSPIKE_NAMESPACE = "AEROSPIKE_NAMESPACE";
    protected static final int AEROSPIKE_DEFAULT_PORT = 3000;
    private static final String CHARGLT_DEFAULT_NAMESPACE = "test";
    private static final String CHARGLT_USERS = "charglt_users";
    private static final String ADD_CREDIT = "ADD_CREDIT";
    private static final String CLEAR_LOCK = "CLEAR_LOCK";
    private static final String CLEAR_UNFINISHED = "CLEAR_UNFINISHED";
    private static final String COUNT_USAGE_TOTAL_BY_DOC = "COUNT_USAGE_TOTAL_BY_DOC";
    private static final String GENERATION_CONFLICT = "GENERATION_CONFLICT";
    private static final String USER_NOT_FOUND = "USER_NOT_FOUND";
    private static final String ASYNC_ERROR = "ASYNC_ERROR";
    private static final int MAX_GENERATION_RETRIES = 10;
    private static final int BATCH_SIZE = 5000;
    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    private static final String namespace = getNamespace();

    /**
     * One event loop per CPU, each with up to MAX_COMMANDS_PER_LOOP requests on the wire at once.
     * More than the server can handle just adds queueing latency without adding throughput.
     */
    public static final String MAX_COMMANDS_PER_LOOP = "MAX_COMMANDS_PER_LOOP";
    private static final int DEFAULT_MAX_COMMANDS_PER_LOOP = 8;
    private static final int EVENT_LOOP_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int MAX_COMMANDS_IN_PROCESS_PER_LOOP = getMaxCommandsPerLoop();
    private static final int MAX_IN_FLIGHT = EVENT_LOOP_COUNT * MAX_COMMANDS_IN_PROCESS_PER_LOOP;

    /**
     * A permit for each request we're allowed to have outstanding. Stops us queuing work faster than
     * Aerospike can do it, and lets us wait for everything to finish.
     */
    private static final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);

    private static EventLoops eventLoops;

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate;
        sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Connect to Aerospike, with event loops for the async API.
     *
     * @param hostlist comma delimited list of host[:port]
     * @return a client. Closing it also closes the event loops.
     */
    protected static IAerospikeClient connect(String hostlist) {

        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = MAX_COMMANDS_IN_PROCESS_PER_LOOP;
        eventLoops = new NioEventLoops(eventPolicy, EVENT_LOOP_COUNT);

        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        clientPolicy.asyncMaxConnsPerNode = MAX_IN_FLIGHT;

        Host[] hosts = Host.parseHosts(hostlist, AEROSPIKE_DEFAULT_PORT);

        msg("Connecting to Aerospike at " + hostlist + ", namespace " + namespace + ", set " + CHARGLT_USERS
                + ", " + EVENT_LOOP_COUNT + " event loops x " + MAX_COMMANDS_IN_PROCESS_PER_LOOP + " commands");

        return new AerospikeClient(clientPolicy, hosts) {
            @Override
            public void close() {
                super.close();
                eventLoops.close();
            }
        };
    }

    /**
     * Wait for a free slot, then return the event loop to send the next request on.
     */
    private static EventLoop startRequest() throws InterruptedException {
        inFlight.acquire();
        return eventLoops.next();
    }

    /**
     * Every request must call this exactly once when it finishes, whether it worked or not.
     */
    private static void endRequest() {
        inFlight.release();
    }

    /**
     * Wait for all outstanding requests to finish.
     */
    private static void drain() throws InterruptedException {
        inFlight.acquire(MAX_IN_FLIGHT);
        inFlight.release(MAX_IN_FLIGHT);
    }


    /**
     * Convenience method to generate a JSON payload.
     *
     * @param length
     * @return
     */
    public static ExtraUserData getExtraUserDataAsObject(int length, Gson gson, Random r) {

        ExtraUserData eud = new ExtraUserData();

        eud.loyaltySchemeName = "HelperCard";
        eud.loyaltySchemeNumber = getNewLoyaltyCardNumber(r);

        StringBuffer ourText = new StringBuffer();

        for (int i = 0; i < length / 2; i++) {
            ourText.append(Integer.toHexString(r.nextInt(256)));
        }

        eud.mysteriousHexPayload = ourText.toString();

        return eud;
    }


    protected static void upsertAllUsers(int userCount, int tpMs, ExtraUserData ourEud, int initialCredit,
                                         IAerospikeClient aerospikeClient, int extraMs)
            throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();

        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);
        Random r = new Random();

        WritePolicy upsertPolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        upsertPolicy.recordExistsAction = RecordExistsAction.REPLACE;

        for (int i = 0; i < userCount; i++) {

            throttle.await();

            UserTable newUser = UserTable.getUserTable(ourEud, r.nextInt(initialCredit), i, startMsUpsert);
            newUser.addCredit(100, "Txn_" + i);
            newUser.reportQuotaUsage(100, 10, 100, "TX2_" + i);

            final long startMs = System.currentTimeMillis();
            final int userId = i;

            EventLoop eventLoop = startRequest();

            try {
                aerospikeClient.put(eventLoop, new WriteListener() {
                    @Override
                    public void onSuccess(Key key) {
                        shc.reportLatency(BaseChargingDemo.ADD_DOC, startMs, "Add time", 2000);
                        shc.incCounter(BaseChargingDemo.ADD_DOC);
                        endRequest();
                    }

                    @Override
                    public void onFailure(AerospikeException e) {
                        msg("Upsert of user " + userId + " failed: " + e.getMessage());
                        shc.incCounter(BaseChargingDemo.ADD_DOC_ERROR);
                        endRequest();
                    }
                }, upsertPolicy, getKey(i), newUser.toBins());
            } catch (AerospikeException e) {
                msg("Upsert of user " + userId + " failed: " + e.getMessage());
                shc.incCounter(BaseChargingDemo.ADD_DOC_ERROR);
                endRequest();
            }

            if (i % 100000 == 1) {
                msg("Upserted " + i + " users...");

                if (shc.getCounter(BaseChargingDemo.ADD_DOC_ERROR) > 0) {
                    msg("Errors detected. Halting...");
                    break;
                } else {
                    queryUserAndStats(aerospikeClient, i);
                }

            }

        }

        drain();

        long entriesPerMS = userCount / Math.max(1, System.currentTimeMillis() - startMsUpsert);
        msg("Upserted " + entriesPerMS + " users per ms...");
        msg(shc.toString());
    }

    private static Key getKey(long i) {
        return new Key(namespace, CHARGLT_USERS, i);
    }

    private static String getNamespace() {
        String ns = System.getenv(AEROSPIKE_NAMESPACE);
        if (ns == null || ns.isEmpty()) {
            return CHARGLT_DEFAULT_NAMESPACE;
        }
        return ns;
    }

    private static int getMaxCommandsPerLoop() {
        String max = System.getenv(MAX_COMMANDS_PER_LOOP);
        if (max == null || max.isEmpty()) {
            return DEFAULT_MAX_COMMANDS_PER_LOOP;
        }
        return Integer.parseInt(max);
    }

    private static Statement getAllUsersStatement() {
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(CHARGLT_USERS);
        return stmt;
    }


    protected static void deleteAllUsers(IAerospikeClient aerospikeClient, int userCount, int tpMs, int extraMs)
            throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();

        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);

        DeleteListener deleteListener = new DeleteListener() {
            @Override
            public void onSuccess(Key key, boolean existed) {
                if (existed) {
                    shc.incCounter(BaseChargingDemo.DELETE_DOC);
                } else {
                    shc.incCounter(BaseChargingDemo.DELETE_DOC_ERROR);
                }
                endRequest();
            }

            @Override
            public void onFailure(AerospikeException e) {
                msg("Delete failed: " + e.getMessage());
                shc.incCounter(BaseChargingDemo.DELETE_DOC_ERROR);
                endRequest();
            }
        };

        for (int i = 0; i < userCount; i++) {

            throttle.await();

            EventLoop eventLoop = startRequest();

            try {
                aerospikeClient.delete(eventLoop, deleteListener, null, getKey(i));
            } catch (AerospikeException e) {
                deleteListener.onFailure(e);
            }

            if (i % 100000 == 1) {
                msg("Deleted " + i + " users...");
            }

        }

        drain();

        long entriesPerMS = userCount / Math.max(1, System.currentTimeMillis() - startMsUpsert);
        msg("Deleted " + entriesPerMS + " users per ms...");
        msg(shc.toString());

    }

    /**
     * Convenience method to query a user a general stats and log the results
     */
    protected static void queryUserAndStats(IAerospikeClient aerospikeClient, int queryUserId) {

        // Query user #queryUserId...
        msg("Query user #" + queryUserId + "...");
        UserTable ut = getUser(aerospikeClient, queryUserId);
        reportDocument(ut);
        msg("Show amount of credit currently reserved for products...");
        getCurrentReservedCredit(aerospikeClient);

    }

    /**
     * Total reserved credit across all users. Each record keeps a 'reserved' bin up to date,
     * so this is a single set query that only returns that bin, rather than a doc by doc fetch.
     */
    private static void getCurrentReservedCredit(IAerospikeClient aerospikeClient) {

        final long getDocByDocMs = System.currentTimeMillis();

        long total = 0;
        long userCount = 0;

        Statement stmt = getAllUsersStatement();
        stmt.setBinNames(UserTable.BIN_RESERVED);

        try (RecordSet rs = aerospikeClient.query(null, stmt)) {
            while (rs.next()) {
                total += rs.getRecord().getLong(UserTable.BIN_RESERVED);
                userCount++;
            }
        }

        msg("Total for " + userCount + " users is " + total);

        shc.reportLatency(BaseChargingDemo.COUNT_USAGE_TOTAL_BY_DOC, getDocByDocMs, "Time to count usage", 10000);
    }

    private static UserTable getUser(IAerospikeClient aerospikeClient, int queryUserId) {
        return UserTable.fromRecord(aerospikeClient.get(null, getKey(queryUserId)));
    }

    /**
     * Read a user, change it, and write it back only if nobody else changed it in the meantime.
     * Aerospike's record generation gives us an atomic compare-and-set; on a conflict we re-read and retry.
     * Both the read and the write are async, so this returns immediately.
     *
     * @param aerospikeClient
     * @param eventLoop       event loop to run on
     * @param userId
     * @param change          changes the UserTable and returns true if it needs to be written
     * @param onDone          called exactly once, with the UserTable as written, or null if the user doesn't
     *                        exist or something went wrong
     */
    private static void readModifyWrite(IAerospikeClient aerospikeClient, EventLoop eventLoop, long userId,
                                        Predicate<UserTable> change, Consumer<UserTable> onDone) {
        readModifyWrite(aerospikeClient, eventLoop, userId, change, onDone, 0);
    }

    private static void readModifyWrite(IAerospikeClient aerospikeClient, EventLoop eventLoop, long userId,
                                        Predicate<UserTable> change, Consumer<UserTable> onDone, int attempt) {

        final Key key = getKey(userId);

        RecordListener readListener = new RecordListener() {
            @Override
            public void onSuccess(Key key, Record rec) {

                try {
                    if (rec == null) {
                        shc.incCounter(USER_NOT_FOUND);
                        msg("User " + userId + " not found");
                        onDone.accept(null);
                        return;
                    }

                    UserTable ut = UserTable.fromRecord(rec);

                    if (!change.test(ut)) {
                        onDone.accept(ut);
                        return;
                    }

                    WritePolicy casPolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    casPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    casPolicy.generation = rec.generation;
                    casPolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

                    aerospikeClient.put(eventLoop, new WriteListener() {
                        @Override
                        public void onSuccess(Key key) {
                            onDone.accept(ut);
                        }

                        @Override
                        public void onFailure(AerospikeException e) {
                            if (e.getResultCode() == ResultCode.GENERATION_ERROR && attempt + 1 < MAX_GENERATION_RETRIES) {
                                shc.incCounter(GENERATION_CONFLICT);
                                readModifyWrite(aerospikeClient, eventLoop, userId, change, onDone, attempt + 1);
                            } else {
                                failed(e);
                            }
                        }
                    }, casPolicy, key, ut.toBins());

                } catch (Exception e) {
                    failed(e);
                }
            }

            @Override
            public void onFailure(AerospikeException e) {
                failed(e);
            }

            private void failed(Exception e) {
                shc.incCounter(ASYNC_ERROR);
                msg("User " + userId + ": " + e.getMessage());
                onDone.accept(null);
            }
        };

        try {
            aerospikeClient.get(eventLoop, readListener, null, key);
        } catch (AerospikeException e) {
            readListener.onFailure(e);
        }
    }


    static protected void reportDocument(UserTable ut) {
        if (ut == null) {
            msg("Document is null...");
        } else {
            msg(ut.toString());

        }
    }


    /**
     *
     * Run a key value store benchmark for userCount users at tpMs transactions per
     * millisecond and with deltaProportion records sending the entire record.
     *
     * @param userCount
     * @param tpMs
     * @param durationSeconds
     * @param globalQueryFreqSeconds
     * @param jsonsize
     * @param mainClient
     * @param deltaProportion
     * @param extraMs
     * @return true if >=90% of requested throughput was achieved.
     * @throws InterruptedException
     */
    protected static boolean runKVBenchmark(int userCount, int tpMs, int durationSeconds, int globalQueryFreqSeconds,
                                            int jsonsize, IAerospikeClient mainClient, int deltaProportion, int extraMs)
            throws InterruptedException {

        UserKVState[] userState = new UserKVState[userCount];

        Random r = new Random();
        Gson gson = new Gson();

        for (int i = 0; i < userCount; i++) {
            userState[i] = new UserKVState(i, shc);
        }

        final long startMsRun = System.currentTimeMillis();
        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        int tranCount = 0;
        int inFlightCount = 0;
        int lockCount = 0;
        int contestedLockCount = 0;
        int fullUpdate = 0;
        int deltaUpdate = 0;

        // Keep an eye on one user while we run...
        final int watchedUser = r.nextInt(userCount);
        ScheduledExecutorService globalQueries = startGlobalQueries(mainClient, watchedUser, globalQueryFreqSeconds);

        try {
            while (endtimeMs > System.currentTimeMillis()) {

                throttle.await();

                // Find session to do a transaction for...
                int oursession = r.nextInt(userCount);

                // See if session already has an active transaction and avoid
                // it if it does.
                if (userState[oursession].isTxInFlight()) {

                    inFlightCount++;

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE) {

                    if (userState[oursession].getOtherLockTimeMs() + ReferenceData.LOCK_TIMEOUT_MS < System
                            .currentTimeMillis()) {

                        userState[oursession].startTran();
                        userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                        GetAndLockUser(mainClient, userState[oursession], oursession);
                        lockCount++;

                    } else {
                        contestedLockCount++;
                    }

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_UNLOCKED) {

                    userState[oursession].startTran();
                    userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                    GetAndLockUser(mainClient, userState[oursession], oursession);
                    lockCount++;

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED) {

                    userState[oursession].startTran();
                    userState[oursession].setStatus(UserKVState.STATUS_UPDATING);

                    if (deltaProportion > r.nextInt(101)) {
                        deltaUpdate++;
                        // Instead of sending entire JSON object across wire ask Aerospike to update loyalty
                        // number inside the userData map bin. For large values this can have a dramatic
                        // effect on network bandwidth
                        UpdateLockedUser(mainClient, userState[oursession],
                                userState[oursession].getLockId(), oursession, getNewLoyaltyCardNumber(r) + "",
                                ExtraUserData.NEW_LOYALTY_NUMBER);
                    } else {
                        fullUpdate++;
                        UpdateLockedUser(mainClient, userState[oursession],
                                userState[oursession].getLockId(), oursession, getExtraUserDataAsObject(jsonsize, gson, r), null);
                    }

                }

                tranCount++;

                if (tranCount % 100000 == 1) {
                    msg("Transaction " + tranCount);
                }

            }

            msg(tranCount + " transactions done...");
            msg("All entries in queue, waiting for it to drain...");
            drain();
            msg("Queue drained...");

        } finally {
            globalQueries.shutdownNow();
        }

        long transactionsPerMs = tranCount / Math.max(1, System.currentTimeMillis() - startMsRun);
        msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");

        long lockFailCount = 0;
        for (int i = 0; i < userCount; i++) {
            lockFailCount += userState[i].getLockedBySomeoneElseCount();
        }

        msg(inFlightCount + " events where a tx was in flight were observed");
        msg(lockCount + " lock attempts");
        msg(contestedLockCount + " contested lock attempts");
        msg(lockFailCount + " lock attempt failures");
        msg(fullUpdate + " full updates");
        msg(deltaUpdate + " delta updates");

        double tps = tranCount;
        tps = tps / (System.currentTimeMillis() - startMsRun);
        tps = tps * 1000;

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    /**
     * Soft lock a user in a single round trip. A filter expression makes the write conditional on
     * the record being unlocked (or the lock having expired), so the check and the lock are atomic.
     */
    private static void GetAndLockUser(IAerospikeClient aerospikeClient, UserKVState userKVState, int sessionId)
            throws InterruptedException {

        final long startMs = System.currentTimeMillis();

        UserTable lockedUser = new UserTable();
        long newLockId = lockedUser.lock();

        WritePolicy lockPolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        lockPolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        lockPolicy.failOnFilteredOut = true;
        lockPolicy.filterExp = Exp.build(Exp.or(
                Exp.eq(Exp.intBin(UserTable.BIN_LOCK_SESSION_ID), Exp.val(NO_SESSION)),
                Exp.lt(Exp.intBin(UserTable.BIN_LOCK_EXPIRY), Exp.val(startMs))));

        RecordListener lockListener = new RecordListener() {
            @Override
            public void onSuccess(Key key, Record rec) {
                userKVState.setLockId(newLockId);
                finished();
            }

            @Override
            public void onFailure(AerospikeException e) {
                if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                    userKVState.setLockId(NO_SESSION);
                    userKVState.setStatus(UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE);
                    userKVState.lockedBySomeoneElseCount++;
                    userKVState.otherLockTimeMs = System.currentTimeMillis();
                } else if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                    msg("User " + sessionId + " not found");
                    userKVState.setLockId(NO_SESSION);
                } else {
                    shc.incCounter(ASYNC_ERROR);
                    msg("Lock of user " + sessionId + " failed: " + e.getMessage());
                    userKVState.setLockId(NO_SESSION);
                }
                finished();
            }

            private void finished() {
                shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);
                // Must be last - tells the sending thread it can use this user again
                userKVState.endTran();
                endRequest();
            }
        };

        EventLoop eventLoop = startRequest();

        try {
            aerospikeClient.operate(eventLoop, lockListener, lockPolicy, getKey(sessionId),
                    Operation.put(new Bin(UserTable.BIN_LOCK_SESSION_ID, newLockId)),
                    Operation.put(new Bin(UserTable.BIN_LOCK_EXPIRY, lockedUser.userSoftlockExpiry.getTime())));
        } catch (AerospikeException e) {
            lockListener.onFailure(e);
        }

    }

    /**
     * Update a user we've locked and release the lock, in a single round trip. The filter expression
     * means nothing is written unless we still hold the lock.
     */
    private static void UpdateLockedUser(IAerospikeClient aerospikeClient, UserKVState userKVState, long lockId,
                                         int sessionId, Object extraPayload, String deltaOperationName)
            throws InterruptedException {

        final long startMs = System.currentTimeMillis();

        WritePolicy updatePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        updatePolicy.failOnFilteredOut = true;
        updatePolicy.filterExp = Exp.build(Exp.eq(Exp.intBin(UserTable.BIN_LOCK_SESSION_ID), Exp.val(lockId)));

        Operation changeUserData;

        if (deltaOperationName != null && deltaOperationName.equals(ExtraUserData.NEW_LOYALTY_NUMBER)) {
            changeUserData = MapOperation.put(MapPolicy.Default, UserTable.BIN_USER_DATA,
                    Value.get(ExtraUserData.LOYALTY_SCHEME_NUMBER), Value.get(Long.parseLong((String) extraPayload)));
        } else {
            changeUserData = Operation.put(new Bin(UserTable.BIN_USER_DATA, ((ExtraUserData) extraPayload).toMap()));
        }

        RecordListener updateListener = new RecordListener() {
            @Override
            public void onSuccess(Key key, Record rec) {
                finished();
            }

            @Override
            public void onFailure(AerospikeException e) {
                if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                    userKVState.lockedBySomeoneElseCount++;
                    msg(sessionId + ": locked by someone else");
                } else if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                    msg("User " + sessionId + " not found");
                } else {
                    shc.incCounter(ASYNC_ERROR);
                    msg("Update of user " + sessionId + " failed: " + e.getMessage());
                }
                finished();
            }

            private void finished() {
                userKVState.setLockId(NO_SESSION);
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
                // Must be last - tells the sending thread it can use this user again
                userKVState.endTran();
                endRequest();
            }
        };

        EventLoop eventLoop = startRequest();

        try {
            aerospikeClient.operate(eventLoop, updateListener, updatePolicy, getKey(sessionId), changeUserData,
                    Operation.put(new Bin(UserTable.BIN_LOCK_SESSION_ID, NO_SESSION)),
                    Operation.put(new Bin(UserTable.BIN_LOCK_EXPIRY, 0L)));
        } catch (AerospikeException e) {
            updateListener.onFailure(e);
        }

    }

    /**
     * Report on one user and global stats every freqSeconds, on a thread of its own so the
     * sending thread isn't held up.
     */
    private static ScheduledExecutorService startGlobalQueries(IAerospikeClient aerospikeClient, int queryUserId,
                                                               int freqSeconds) {

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                queryUserAndStats(aerospikeClient, queryUserId);
            } catch (Exception e) {
                msg("Global query failed: " + e.getMessage());
            }
        }, freqSeconds, Math.max(1, freqSeconds), TimeUnit.SECONDS);

        return scheduler;
    }

    /**
     * Find every user matching filter and apply ops to it. The filter is evaluated server side,
     * so only matching users come back across the wire.
     *
     * @return how many users were changed
     */
    private static long updateMatchingUsers(IAerospikeClient aerospikeClient, Expression filter, Operation... ops) {

        QueryPolicy queryPolicy = new QueryPolicy(aerospikeClient.getQueryPolicyDefault());
        queryPolicy.filterExp = filter;
        queryPolicy.includeBinData = false;

        BatchWritePolicy updatePolicy = new BatchWritePolicy();
        updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        long changed = 0;
        List<Key> keys = new ArrayList<>(BATCH_SIZE);

        try (RecordSet rs = aerospikeClient.query(queryPolicy, getAllUsersStatement())) {
            while (rs.next()) {
                keys.add(rs.getKey());
                if (keys.size() == BATCH_SIZE) {
                    changed += batchUpdate(aerospikeClient, updatePolicy, keys, ops);
                }
            }
        }

        return changed + batchUpdate(aerospikeClient, updatePolicy, keys, ops);
    }

    private static long batchUpdate(IAerospikeClient aerospikeClient, BatchWritePolicy updatePolicy, List<Key> keys, Operation... ops) {

        if (keys.isEmpty()) {
            return 0;
        }

        BatchResults results = aerospikeClient.operate(null, updatePolicy, keys.toArray(new Key[0]), ops);
        keys.clear();

        long changed = 0;
        for (BatchRecord br : results.records) {
            if (br.resultCode == ResultCode.OK) {
                changed++;
            }
        }
        return changed;
    }

    /**
     * Convenience method to remove unneeded records storing old allotments of
     * credit.
     *
     */
    protected static void clearUnfinishedTransactions(IAerospikeClient aerospikeClient)
            throws Exception {

        msg("clearUnfinishedTransactions...");
        final long startMs = System.currentTimeMillis();

        long changed = updateMatchingUsers(aerospikeClient,
                Exp.build(Exp.gt(Exp.intBin(UserTable.BIN_RESERVED), Exp.val(0))),
                Operation.put(new Bin(UserTable.BIN_USAGE, UserTable.GSON.toJson(new HashMap<Long, UserUsageTable>()))),
                Operation.put(new Bin(UserTable.BIN_RESERVED, 0L)));

        shc.reportLatency(BaseChargingDemo.CLEAR_UNFINISHED, startMs, "clear unfinished", 2000);
        msg("...done. Cleared " + changed + " users");

    }

    /**
     *
     * Convenience method to clear outstanding locks between runs
     *
     * @param aerospikeClient
     */
    protected static void unlockAllRecords(IAerospikeClient aerospikeClient) {

        msg("Clearing locked sessions from prior runs...");
        final long startMs = System.currentTimeMillis();

        long changed = updateMatchingUsers(aerospikeClient,
                Exp.build(Exp.ne(Exp.intBin(UserTable.BIN_LOCK_SESSION_ID), Exp.val(NO_SESSION))),
                Operation.put(new Bin(UserTable.BIN_LOCK_SESSION_ID, NO_SESSION)),
                Operation.put(new Bin(UserTable.BIN_LOCK_EXPIRY, NO_EXPIRY.getTime())));

        shc.reportLatency(BaseChargingDemo.CLEAR_LOCK, startMs, "clear locks", 2000);
        msg("...done. Unlocked " + changed + " users");

    }


    /**
     *
     * Run a transaction benchmark for userCount users at tpMs per ms.
     *
     * @param userCount              number of users
     * @param tpMs                   transactions per milliseconds
     * @param durationSeconds
     * @param globalQueryFreqSeconds how often we check on global stats and a single
     *                               user
     * @param mainClient
     * @return true if within 90% of targeted TPS
     * @throws InterruptedException
     */
    protected static boolean runTransactionBenchmark(int userCount, int tpMs, int durationSeconds,
                                                     int globalQueryFreqSeconds, IAerospikeClient mainClient, int extraMs)
            throws InterruptedException {

        // Used to track changes and be unique when we are running multiple processes
        final long pid = getPid();

        Random r = new Random();

        UserTransactionState[] users = new UserTransactionState[userCount];

        msg("Creating internal client records for " + users.length + " users");
        for (int i = 0; i < users.length; i++) {
            // We don't know a users credit till we've spoken to the server, so
            // we make an optimistic assumption...
            users[i] = new UserTransactionState(i, 2000);
        }

        final long startMsRun = System.currentTimeMillis();
        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        long tranCount = 0;
        long inFlightCount = 0;
        long addCreditCount = 0;
        long reportUsageCount = 0;

        msg("starting...");

        ScheduledExecutorService globalQueries = startGlobalQueries(mainClient, (int) GENERIC_QUERY_USER_ID,
                globalQueryFreqSeconds);

        try {
            while (endtimeMs > System.currentTimeMillis()) {

                throttle.await();

                int randomuser = r.nextInt(userCount);

                if (users[randomuser].isTxInFlight()) {
                    inFlightCount++;
                } else {

                    users[randomuser].startTran();

                    if (users[randomuser].spendableBalance < 1000) {

                        addCreditCount++;

                        final long extraCredit = r.nextInt(1000) + 1000;
                        addCredit(mainClient, randomuser, extraCredit, users[randomuser]);

                    } else {

                        reportUsageCount++;

                        int unitsUsed = (int) (users[randomuser].currentlyReserved * 0.9);
                        int unitsWanted = r.nextInt(100);
                        reportQuotaUsage(mainClient, randomuser, unitsUsed,
                                unitsWanted, users[randomuser].sessionId,
                                "ReportQuotaUsage_" + pid + "_" + reportUsageCount + "_" + System.currentTimeMillis(),
                                users[randomuser]);

                    }
                }

                if (tranCount++ % 100000 == 0) {
                    msg("On transaction #" + tranCount);
                }

            }

            msg("finished adding transactions to queue");
            drain();
            msg("Queue drained");

        } finally {
            globalQueries.shutdownNow();
        }

        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
        msg("Processed " + tranCount + " transactions in " + elapsedTimeMs + " milliseconds");

        double tps = tranCount;
        tps = tps / (elapsedTimeMs / 1000d);

        msg("TPS = " + tps);

        msg("Add Credit calls = " + addCreditCount);
        msg("Report Usage calls = " + reportUsageCount);
        msg("Skipped because transaction was in flight = " + inFlightCount);
        msg("Retries caused by concurrent changes = " + shc.getCounter(GENERATION_CONFLICT));
        msg("Errors = " + shc.getCounter(ASYNC_ERROR));

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }


    private static void addCredit(IAerospikeClient aerospikeClient, int randomuser, long extraCredit,
                                  UserTransactionState userTransactionState) throws InterruptedException {

        final long startMs = System.currentTimeMillis();
        final String txnId = "AC_" + getPid() + "_" + randomuser + "_" + System.nanoTime();

        readModifyWrite(aerospikeClient, startRequest(), randomuser, theUser -> {
            theUser.addCredit(extraCredit, txnId);
            return true;
        }, ut -> {
            if (ut != null) {
                userTransactionState.spendableBalance = ut.getAvailableCredit();
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
            }
            shc.reportLatency(BaseChargingDemo.ADD_CREDIT, startMs, "ADD_CREDIT", 2000);
            shc.incCounter(BaseChargingDemo.ADD_CREDIT);
            // Must be last - tells the sending thread it can use this user again
            userTransactionState.endTran();
            endRequest();
        });
    }


    private static void reportQuotaUsage(IAerospikeClient aerospikeClient
            , int randomuser, int unitsUsed, int unitsWanted, long sessionId
            , String txnId, UserTransactionState userTransactionState) throws InterruptedException {


        final long startMs = System.currentTimeMillis();

        readModifyWrite(aerospikeClient, startRequest(), randomuser, theUser -> {
            theUser.reportQuotaUsage(unitsUsed, unitsWanted, sessionId, txnId);
            return true;
        }, ut -> {
            if (ut != null) {
                userTransactionState.spendableBalance = ut.getAvailableCredit();
                UserUsageTable reservation = ut.getUserUsage(sessionId);
                userTransactionState.currentlyReserved = reservation == null ? 0 : reservation.allocatedAmount;
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
            }
            shc.reportLatency(BaseChargingDemo.REPORT_QUOTA_USAGE, startMs, "REPORT_QUOTA_USAGE", 2000);
            shc.incCounter(BaseChargingDemo.REPORT_QUOTA_USAGE);
            // Must be last - tells the sending thread it can use this user again
            userTransactionState.endTran();
            endRequest();
        });
    }


    /**
     * Turn latency stats into a grepable string
     *
     * @param tpMs target transactions per millisecond
     * @param tps  observed TPS
     */
    private static void reportRunLatencyStats(int tpMs, double tps) {
        StringBuffer oneLineSummary = new StringBuffer("GREPABLE SUMMARY:");

        oneLineSummary.append(tpMs);
        oneLineSummary.append(':');

        oneLineSummary.append(tps);
        oneLineSummary.append(':');

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, REPORT_QUOTA_USAGE);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_PUT);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_GET);

        msg(oneLineSummary.toString());

        msg(shc.toString());
    }

    /**
     * Get Linux process ID - used for pseudo unique ids
     *
     * @return Linux process ID
     */
    private static long getPid() {
        return ProcessHandle.current().pid();
    }

    /**
     * Return a loyalty card number
     *
     * @param r instance of Random
     * @return a random loyalty card number between 0 and 1 million
     */
    private static long getNewLoyaltyCardNumber(Random r) {
        return System.currentTimeMillis() % 1000000;
    }

    /**
     * get EXTRA_MS env variable if set
     *
     * @return extraMs
     */
    public static int getExtraMsIfSet() {

        int extraMs = 0;

        String extraMsEnv = System.getenv(EXTRA_MS);

        if (extraMsEnv != null && !extraMsEnv.isEmpty()) {
            msg("EXTRA_MS is '" + extraMsEnv + "'");
            extraMs = Integer.parseInt(extraMsEnv);
        }

        return extraMs;
    }

}
