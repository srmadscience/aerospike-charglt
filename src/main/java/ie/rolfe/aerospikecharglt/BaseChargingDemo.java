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
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.Predicate;

/**
 * This is an abstract class that contains the actual logic of the demo code.
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
    private static final int MAX_GENERATION_RETRIES = 10;
    private static final int BATCH_SIZE = 5000;
    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    private static final String namespace = getNamespace();

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
     * Connect to Aerospike.
     *
     * @param hostlist comma delimited list of host[:port]
     * @return a client
     */
    protected static IAerospikeClient connect(String hostlist) {

        ClientPolicy clientPolicy = new ClientPolicy();
        Host[] hosts = Host.parseHosts(hostlist, AEROSPIKE_DEFAULT_PORT);

        msg("Connecting to Aerospike at " + hostlist + ", namespace " + namespace + ", set " + CHARGLT_USERS);
        return new AerospikeClient(clientPolicy, hosts);
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
                                         IAerospikeClient aerospikeClient, int threadCount, int extraMs)
            throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();
        final int usableThreadCount = getUsableThreadCount(threadCount, tpMs, userCount);
        final AtomicLong upsertCount = new AtomicLong();

        WritePolicy upsertPolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        upsertPolicy.recordExistsAction = RecordExistsAction.REPLACE;

        msg("Upserting " + userCount + " users using " + usableThreadCount + " threads");

        runInThreads(usableThreadCount, threadId -> () -> {

            TpMsThrottle throttle = new TpMsThrottle(getThreadTpMs(tpMs, usableThreadCount, threadId), extraMs);
            Random r = new Random();

            for (int i = threadId; i < userCount; i += usableThreadCount) {

                throttle.await();

                UserTable newUser = UserTable.getUserTable(ourEud, r.nextInt(initialCredit), i, startMsUpsert);
                newUser.addCredit(100, "Txn_" + i);
                newUser.reportQuotaUsage(100, 10, 100, "TX2_" + i);

                final long startMs = System.currentTimeMillis();

                try {
                    aerospikeClient.put(upsertPolicy, getKey(i), newUser.toBins());
                    shc.reportLatency(BaseChargingDemo.ADD_DOC, startMs, "Add time", 2000);
                    shc.incCounter(BaseChargingDemo.ADD_DOC);
                } catch (AerospikeException e) {
                    msg("Upsert of user " + i + " failed: " + e.getMessage());
                    shc.incCounter(BaseChargingDemo.ADD_DOC_ERROR);
                }

                long upserted = upsertCount.incrementAndGet();

                if (upserted % 100000 == 1) {
                    msg("Upserted " + upserted + " users...");

                    if (shc.getCounter(BaseChargingDemo.ADD_DOC_ERROR) > 0) {
                        msg("Errors detected. Halting...");
                        break;
                    } else if (threadId == 0) {
                        queryUserAndStats(aerospikeClient, i);
                    }

                }

            }

            return null;
        });


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

    private static Statement getAllUsersStatement() {
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(CHARGLT_USERS);
        return stmt;
    }


    protected static void deleteAllUsers(IAerospikeClient aerospikeClient, int userCount, int tpMs, int threadCount,
                                         int extraMs) throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();
        final int usableThreadCount = getUsableThreadCount(threadCount, tpMs, userCount);
        final AtomicLong deleteCount = new AtomicLong();

        msg("Deleting " + userCount + " users using " + usableThreadCount + " threads");

        runInThreads(usableThreadCount, threadId -> () -> {

            TpMsThrottle throttle = new TpMsThrottle(getThreadTpMs(tpMs, usableThreadCount, threadId), extraMs);

            for (int i = threadId; i < userCount; i += usableThreadCount) {

                throttle.await();

                long startNs = System.currentTimeMillis();
                boolean existed = aerospikeClient.delete(null, getKey(i));
                shc.reportLatency(BaseChargingDemo.DELETE_DOC, startNs, "Delete time", 2000);
                if (existed) {
                    shc.incCounter(BaseChargingDemo.DELETE_DOC);
                } else {
                    shc.incCounter(BaseChargingDemo.DELETE_DOC_ERROR);
                }

                long deleted = deleteCount.incrementAndGet();
                if (deleted % 100000 == 1) {
                    msg("Deleted " + deleted + " users...");
                }

            }

            return null;
        });

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
     *
     * @param aerospikeClient
     * @param userId
     * @param change          changes the UserTable and returns true if it needs to be written
     * @return the UserTable as written, or null if the user doesn't exist
     */
    private static UserTable readModifyWrite(IAerospikeClient aerospikeClient, long userId, Predicate<UserTable> change) {

        final Key key = getKey(userId);

        for (int attempt = 0; attempt < MAX_GENERATION_RETRIES; attempt++) {

            Record rec = aerospikeClient.get(null, key);

            if (rec == null) {
                shc.incCounter(USER_NOT_FOUND);
                msg("User " + userId + " not found");
                return null;
            }

            UserTable ut = UserTable.fromRecord(rec);

            if (!change.test(ut)) {
                return ut;
            }

            WritePolicy casPolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            casPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            casPolicy.generation = rec.generation;
            casPolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

            try {
                aerospikeClient.put(casPolicy, key, ut.toBins());
                return ut;
            } catch (AerospikeException e) {
                if (e.getResultCode() != ResultCode.GENERATION_ERROR) {
                    throw e;
                }
                shc.incCounter(GENERATION_CONFLICT);
            }
        }

        throw new IllegalStateException("User " + userId + " still changing after " + MAX_GENERATION_RETRIES + " attempts");
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
     * @param threadCount            how many threads to spread the work over
     * @return true if >=90% of requested throughput was achieved.
     * @throws InterruptedException
     */
    protected static boolean runKVBenchmark(int userCount, int tpMs, int durationSeconds, int globalQueryFreqSeconds,
                                            int jsonsize, IAerospikeClient mainClient, int deltaProportion, int extraMs,
                                            int threadCount)
            throws InterruptedException {

        final int usableThreadCount = getUsableThreadCount(threadCount, tpMs, userCount);
        final AtomicLong progressCount = new AtomicLong();

        final long startMsRun = System.currentTimeMillis();
        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        msg("Running KV benchmark for " + userCount + " users using " + usableThreadCount + " threads");

        // Keep an eye on one user while we run...
        final int watchedUser = new Random().nextInt(userCount);
        ScheduledExecutorService globalQueries = startGlobalQueries(mainClient, watchedUser, globalQueryFreqSeconds);

        KVResults results = new KVResults();

        try {
            for (KVResults threadResults : runInThreads(usableThreadCount, threadId -> () ->
                    runKVWorker(threadId, usableThreadCount, userCount, tpMs, endtimeMs, jsonsize, mainClient,
                            deltaProportion, extraMs, progressCount))) {
                results.add(threadResults);
            }
        } finally {
            globalQueries.shutdownNow();
        }

        msg(results.tranCount + " transactions done...");

        long transactionsPerMs = results.tranCount / Math.max(1, System.currentTimeMillis() - startMsRun);
        msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");

        msg(results.inFlightCount + " events where a tx was in flight were observed");
        msg(results.lockCount + " lock attempts");
        msg(results.contestedLockCount + " contested lock attempts");
        msg(results.lockFailCount + " lock attempt failures");
        msg(results.fullUpdate + " full updates");
        msg(results.deltaUpdate + " delta updates");

        double tps = results.tranCount;
        tps = tps / (System.currentTimeMillis() - startMsRun);
        tps = tps * 1000;

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    /**
     * One thread's share of the KV benchmark. It owns every user whose id % threadCount == threadId,
     * so no other thread in this process touches its users.
     */
    private static KVResults runKVWorker(int threadId, int threadCount, int userCount, int tpMs, long endtimeMs,
                                         int jsonsize, IAerospikeClient mainClient, int deltaProportion, int extraMs,
                                         AtomicLong progressCount)
            throws InterruptedException {

        UserKVState[] userState = new UserKVState[getThreadUserCount(userCount, threadCount, threadId)];

        for (int i = 0; i < userState.length; i++) {
            userState[i] = new UserKVState(i * threadCount + threadId, shc);
        }

        Random r = new Random();
        Gson gson = new Gson();
        TpMsThrottle throttle = new TpMsThrottle(getThreadTpMs(tpMs, threadCount, threadId), extraMs);
        KVResults results = new KVResults();

        while (endtimeMs > System.currentTimeMillis()) {

            throttle.await();

            // Find session to do a transaction for...
            UserKVState ourState = userState[r.nextInt(userState.length)];
            int oursession = ourState.id;

            // See if session already has an active transaction and avoid
            // it if it does.
            if (ourState.isTxInFlight()) {

                results.inFlightCount++;

            } else if (ourState.getUserStatus() == UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE) {

                if (ourState.getOtherLockTimeMs() + ReferenceData.LOCK_TIMEOUT_MS < System
                        .currentTimeMillis()) {

                    ourState.startTran();
                    ourState.setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                    GetAndLockUser(mainClient, ourState, oursession);
                    results.lockCount++;

                } else {
                    results.contestedLockCount++;
                }

            } else if (ourState.getUserStatus() == UserKVState.STATUS_UNLOCKED) {

                ourState.startTran();
                ourState.setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                GetAndLockUser(mainClient, ourState, oursession);
                results.lockCount++;

            } else if (ourState.getUserStatus() == UserKVState.STATUS_LOCKED) {

                ourState.startTran();
                ourState.setStatus(UserKVState.STATUS_UPDATING);

                if (deltaProportion > r.nextInt(101)) {
                    results.deltaUpdate++;
                    // Instead of sending entire JSON object across wire ask Aerospike to update loyalty
                    // number inside the userData map bin. For large values this can have a dramatic
                    // effect on network bandwidth
                    UpdateLockedUser(mainClient, ourState, ourState.getLockId(), oursession,
                            getNewLoyaltyCardNumber(r) + "", ExtraUserData.NEW_LOYALTY_NUMBER);
                } else {
                    results.fullUpdate++;
                    UpdateLockedUser(mainClient, ourState, ourState.getLockId(), oursession,
                            getExtraUserDataAsObject(jsonsize, gson, r), null);
                }

            }

            results.tranCount++;
            ourState.endTran(); //TODO - Fix when we make async

            long progress = progressCount.incrementAndGet();
            if (progress % 100000 == 1) {
                msg("Transaction " + progress);
            }

        }

        for (UserKVState state : userState) {
            results.lockFailCount += state.getLockedBySomeoneElseCount();
        }

        return results;
    }

    /**
     * Soft lock a user in a single round trip. A filter expression makes the write conditional on
     * the record being unlocked (or the lock having expired), so the check and the lock are atomic.
     */
    private static void GetAndLockUser(IAerospikeClient aerospikeClient, UserKVState userKVState, int sessionId) {

        final long startMs = System.currentTimeMillis();

        UserTable lockedUser = new UserTable();
        long newLockId = lockedUser.lock();

        WritePolicy lockPolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        lockPolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        lockPolicy.failOnFilteredOut = true;
        lockPolicy.filterExp = Exp.build(Exp.or(
                Exp.eq(Exp.intBin(UserTable.BIN_LOCK_SESSION_ID), Exp.val(NO_SESSION)),
                Exp.lt(Exp.intBin(UserTable.BIN_LOCK_EXPIRY), Exp.val(startMs))));

        try {
            aerospikeClient.operate(lockPolicy, getKey(sessionId),
                    Operation.put(new Bin(UserTable.BIN_LOCK_SESSION_ID, newLockId)),
                    Operation.put(new Bin(UserTable.BIN_LOCK_EXPIRY, lockedUser.userSoftlockExpiry.getTime())));
            userKVState.setLockId(newLockId);

        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                userKVState.setLockId(NO_SESSION);
                userKVState.setStatus(UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE);
                userKVState.lockedBySomeoneElseCount++;
                userKVState.otherLockTimeMs = System.currentTimeMillis();
            } else if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                msg("User " + sessionId + " not found");
                userKVState.setLockId(NO_SESSION);
            } else {
                e.printStackTrace();
                userKVState.setLockId(NO_SESSION);
            }
        }

        shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);

    }

    /**
     * Update a user we've locked and release the lock, in a single round trip. The filter expression
     * means nothing is written unless we still hold the lock.
     */
    private static void UpdateLockedUser(IAerospikeClient aerospikeClient, UserKVState userKVState, long lockId,
                                         int sessionId, Object extraPayload, String deltaOperationName) {

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

        try {
            aerospikeClient.operate(updatePolicy, getKey(sessionId), changeUserData,
                    Operation.put(new Bin(UserTable.BIN_LOCK_SESSION_ID, NO_SESSION)),
                    Operation.put(new Bin(UserTable.BIN_LOCK_EXPIRY, 0L)));

        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                userKVState.lockedBySomeoneElseCount++;
                msg(sessionId + ": locked by someone else");
            } else if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                msg("User " + sessionId + " not found");
            } else {
                e.printStackTrace();
            }
        }

        userKVState.setLockId(NO_SESSION);
        shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);

    }

    /**
     * Run one worker per thread and wait for them all to finish.
     *
     * @param threadCount     how many threads
     * @param workerForThread given a thread id, returns the work for that thread
     * @return each thread's result, in thread id order
     */
    protected static <T> List<T> runInThreads(int threadCount, IntFunction<Callable<T>> workerForThread)
            throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        try {
            List<Callable<T>> workers = new ArrayList<>(threadCount);
            for (int threadId = 0; threadId < threadCount; threadId++) {
                workers.add(workerForThread.apply(threadId));
            }

            List<T> results = new ArrayList<>(threadCount);
            for (Future<T> future : pool.invokeAll(workers)) {
                try {
                    results.add(future.get());
                } catch (ExecutionException e) {
                    throw new RuntimeException(e.getCause());
                }
            }
            return results;

        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Each thread needs at least one tx per ms and one user to work with.
     */
    protected static int getUsableThreadCount(int threadCount, int tpMs, int userCount) {
        return Math.max(1, Math.min(threadCount, Math.min(tpMs, userCount)));
    }

    /**
     * @return threadId's share of tpMs
     */
    private static int getThreadTpMs(int tpMs, int threadCount, int threadId) {
        return tpMs / threadCount + (threadId < tpMs % threadCount ? 1 : 0);
    }

    /**
     * @return how many users have id % threadCount == threadId
     */
    private static int getThreadUserCount(int userCount, int threadCount, int threadId) {
        return (userCount - threadId + threadCount - 1) / threadCount;
    }

    /**
     * Report on one user and global stats every freqSeconds, on a thread of its own so the
     * workers aren't held up.
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
     * @param threadCount            how many threads to spread the work over
     * @return true if within 90% of targeted TPS
     * @throws InterruptedException
     */
    protected static boolean runTransactionBenchmark(int userCount, int tpMs, int durationSeconds,
                                                     int globalQueryFreqSeconds, IAerospikeClient mainClient, int extraMs,
                                                     int threadCount)
            throws InterruptedException {

        final int usableThreadCount = getUsableThreadCount(threadCount, tpMs, userCount);
        final AtomicLong progressCount = new AtomicLong();

        final long startMsRun = System.currentTimeMillis();
        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        msg("starting " + usableThreadCount + " threads for " + userCount + " users...");

        ScheduledExecutorService globalQueries = startGlobalQueries(mainClient, (int) GENERIC_QUERY_USER_ID,
                globalQueryFreqSeconds);

        TransactionResults results = new TransactionResults();

        try {
            for (TransactionResults threadResults : runInThreads(usableThreadCount, threadId -> () ->
                    runTransactionWorker(threadId, usableThreadCount, userCount, tpMs, endtimeMs, mainClient, extraMs,
                            progressCount))) {
                results.add(threadResults);
            }
        } finally {
            globalQueries.shutdownNow();
        }

        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
        msg("Processed " + results.tranCount + " transactions in " + elapsedTimeMs + " milliseconds");

        double tps = results.tranCount;
        tps = tps / (elapsedTimeMs / 1000d);

        msg("TPS = " + tps);

        msg("Add Credit calls = " + results.addCreditCount);
        msg("Report Usage calls = " + results.reportUsageCount);
        msg("Skipped because transaction was in flight = " + results.inFlightCount);
        msg("Retries caused by concurrent changes = " + shc.getCounter(GENERATION_CONFLICT));

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    /**
     * One thread's share of the transaction benchmark. It owns every user whose id % threadCount == threadId,
     * so no other thread in this process touches its users.
     */
    private static TransactionResults runTransactionWorker(int threadId, int threadCount, int userCount, int tpMs,
                                                           long endtimeMs, IAerospikeClient mainClient, int extraMs,
                                                           AtomicLong progressCount)
            throws InterruptedException {

        // Used to track changes and be unique when we are running multiple threads
        final String txnPrefix = "ReportQuotaUsage_" + getPid() + "_" + threadId + "_";

        Random r = new Random();

        UserTransactionState[] users = new UserTransactionState[getThreadUserCount(userCount, threadCount, threadId)];

        for (int i = 0; i < users.length; i++) {
            // We don't know a users credit till we've spoken to the server, so
            // we make an optimistic assumption...
            users[i] = new UserTransactionState(i * threadCount + threadId, 2000);
        }

        TpMsThrottle throttle = new TpMsThrottle(getThreadTpMs(tpMs, threadCount, threadId), extraMs);
        TransactionResults results = new TransactionResults();

        while (endtimeMs > System.currentTimeMillis()) {

            throttle.await();

            UserTransactionState user = users[r.nextInt(users.length)];

            if (user.isTxInFlight()) {
                results.inFlightCount++;
            } else {

                user.startTran();

                if (user.spendableBalance < 1000) {

                    results.addCreditCount++;

                    final long extraCredit = r.nextInt(1000) + 1000;

                    final long startMs = System.currentTimeMillis();
                    addCredit(mainClient, user.id, extraCredit, user);
                    shc.reportLatency(BaseChargingDemo.ADD_CREDIT, startMs, "ADD_CREDIT", 2000);
                    shc.incCounter(BaseChargingDemo.ADD_CREDIT);
                    user.endTran();


                } else {

                    results.reportUsageCount++;

                    int unitsUsed = (int) (user.currentlyReserved * 0.9);
                    int unitsWanted = r.nextInt(100);
                    final long startMs = System.currentTimeMillis();
                    reportQuotaUsage(mainClient, user.id, unitsUsed, unitsWanted, user.sessionId,
                            txnPrefix + results.reportUsageCount + "_" + System.currentTimeMillis(), user);
                    shc.reportLatency(BaseChargingDemo.REPORT_QUOTA_USAGE, startMs, "REPORT_QUOTA_USAGE", 2000);
                    shc.incCounter(BaseChargingDemo.REPORT_QUOTA_USAGE);
                    user.endTran();

                }
            }

            results.tranCount++;

            long progress = progressCount.incrementAndGet();
            if (progress % 100000 == 1) {
                msg("On transaction #" + progress);
            }

        }

        return results;
    }

    /**
     * Counters from one KV benchmark thread
     */
    private static class KVResults {
        long tranCount = 0;
        long inFlightCount = 0;
        long lockCount = 0;
        long contestedLockCount = 0;
        long lockFailCount = 0;
        long fullUpdate = 0;
        long deltaUpdate = 0;

        void add(KVResults other) {
            tranCount += other.tranCount;
            inFlightCount += other.inFlightCount;
            lockCount += other.lockCount;
            contestedLockCount += other.contestedLockCount;
            lockFailCount += other.lockFailCount;
            fullUpdate += other.fullUpdate;
            deltaUpdate += other.deltaUpdate;
        }
    }

    /**
     * Counters from one transaction benchmark thread
     */
    private static class TransactionResults {
        long tranCount = 0;
        long inFlightCount = 0;
        long addCreditCount = 0;
        long reportUsageCount = 0;

        void add(TransactionResults other) {
            tranCount += other.tranCount;
            inFlightCount += other.inFlightCount;
            addCreditCount += other.addCreditCount;
            reportUsageCount += other.reportUsageCount;
        }
    }


    private static void addCredit(IAerospikeClient aerospikeClient, int randomuser, long extraCredit,
                                  UserTransactionState userTransactionState) {

        final long startMs = System.currentTimeMillis();
        final String txnId = "AC_" + getPid() + "_" + randomuser + "_" + System.nanoTime();

        try {
            UserTable ut = readModifyWrite(aerospikeClient, randomuser, theUser -> {
                theUser.addCredit(extraCredit, txnId);
                return true;
            });

            if (ut != null) {
                userTransactionState.spendableBalance = ut.getAvailableCredit();
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void reportQuotaUsage(IAerospikeClient aerospikeClient
            , int randomuser, int unitsUsed, int unitsWanted, long sessionId
            , String txnId, UserTransactionState userTransactionState) {


        final long startMs = System.currentTimeMillis();

        try {
            UserTable ut = readModifyWrite(aerospikeClient, randomuser, theUser -> {
                theUser.reportQuotaUsage(unitsUsed, unitsWanted, sessionId, txnId);
                return true;
            });

            if (ut != null) {
                userTransactionState.spendableBalance = ut.getAvailableCredit();
                UserUsageTable reservation = ut.getUserUsage(sessionId);
                userTransactionState.currentlyReserved = reservation == null ? 0 : reservation.allocatedAmount;
                shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
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
     * Get the optional thread count argument.
     *
     * @param args  command line
     * @param index where the thread count would be
     * @return thread count, or the number of CPUs if not given
     */
    public static int getThreadCount(String[] args, int index) {

        if (args.length > index) {
            return Integer.parseInt(args[index]);
        }

        return Runtime.getRuntime().availableProcessors();
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
