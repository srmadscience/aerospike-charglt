/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.aerospikecharglt.documents;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import ie.rolfe.aerospikecharglt.ReferenceData;

import java.lang.reflect.Type;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * A user and everything we know about them. Stored as a single Aerospike record keyed by userId.
 * Scalars are stored as their own bins, userDataObject as a Map bin, and the two collections as JSON strings.
 */
public class UserTable {

    public static final long FIVE_MINUTES_IN_MS = 1000 * 60 * 5;
    public static final String ADDED_BY_TXN = " added by Txn ";
    public static final String ALREADY_HAPPENED = " already happened";
    public static long TX_KEEP_MS = 300000;

    // Aerospike bin names - max 15 chars
    public static final String BIN_USER_ID = "userId";
    public static final String BIN_LAST_SEEN = "lastSeen";
    public static final String BIN_LOCK_SESSION_ID = "lockSessionId";
    public static final String BIN_LOCK_EXPIRY = "lockExpiry";
    public static final String BIN_BALANCE = "balance";
    public static final String BIN_RESERVED = "reserved";
    public static final String BIN_USER_DATA = "userData";
    public static final String BIN_USAGE = "usage";
    public static final String BIN_TXNS = "txns";

    /**
     * Dates are stored as epoch ms, which avoids locale-dependent Gson date formats.
     */
    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Date.class, (JsonSerializer<Date>) (src, typeOfSrc, context) -> new JsonPrimitive(src.getTime()))
            .registerTypeAdapter(Date.class, (JsonDeserializer<Date>) (json, typeOfT, context) -> new Date(json.getAsLong()))
            .create();

    private static final Type USAGE_TYPE = new TypeToken<HashMap<Long, UserUsageTable>>() {
    }.getType();
    private static final Type TXNS_TYPE = new TypeToken<HashMap<String, UserRecentTransactions>>() {
    }.getType();

    public long userId;
    public ExtraUserData userDataObject;
    public Date userLastSeen;
    public long userSoftLockSessionId = Long.MIN_VALUE;
    public Date userSoftlockExpiry;
    public HashMap<Long, UserUsageTable> userUsage = new HashMap<Long, UserUsageTable>();
    public HashMap<String, UserRecentTransactions> userRecentTransactions = new HashMap<String, UserRecentTransactions>();
    public long balance = 0;

    public UserTable(long userId, ExtraUserData userJsonObject, Date userLastSeen, Date userSoftlockExpiry, long userSoftLockSessionId) {
        this.userId = userId;
        this.userDataObject = userJsonObject;
        this.userLastSeen = userLastSeen;
        this.userSoftlockExpiry = userSoftlockExpiry;
        this.userSoftLockSessionId = userSoftLockSessionId;
    }

    public UserTable() {

    }

    public static UserTable fromJson(Gson gson, String document) {
        return GSON.fromJson(document, UserTable.class);
    }

    public static UserTable getUserTable(ExtraUserData eud, long initialCredit, long id, long startMsUpsert) {
        String txnId = "Create_" + id;
        final long approvedAmount = 0;
        String purpose = "Created";
        Date createDate = new Date(startMsUpsert);

        UserTable newUser = new UserTable(id, eud, createDate, null, Long.MIN_VALUE);

        UserRecentTransactions urt = new UserRecentTransactions(id, txnId, createDate, Long.MIN_VALUE, approvedAmount, initialCredit, purpose);
        newUser.addUserRecentTransaction(urt);

        return newUser;
    }

    /**
     * @return this user as Aerospike bins
     */
    public Bin[] toBins() {
        return new Bin[]{
                new Bin(BIN_USER_ID, userId),
                new Bin(BIN_LAST_SEEN, userLastSeen == null ? 0 : userLastSeen.getTime()),
                new Bin(BIN_LOCK_SESSION_ID, userSoftLockSessionId),
                new Bin(BIN_LOCK_EXPIRY, userSoftlockExpiry == null ? 0 : userSoftlockExpiry.getTime()),
                new Bin(BIN_BALANCE, balance),
                new Bin(BIN_RESERVED, getUsageBalance()),
                new Bin(BIN_USER_DATA, userDataObject == null ? null : userDataObject.toMap()),
                new Bin(BIN_USAGE, GSON.toJson(userUsage, USAGE_TYPE)),
                new Bin(BIN_TXNS, GSON.toJson(userRecentTransactions, TXNS_TYPE))
        };
    }

    /**
     * @param rec an Aerospike record written by toBins()
     * @return a UserTable, or null if rec is null
     */
    public static UserTable fromRecord(Record rec) {

        if (rec == null) {
            return null;
        }

        UserTable ut = new UserTable();
        ut.userId = rec.getLong(BIN_USER_ID);
        ut.userLastSeen = getDate(rec.getLong(BIN_LAST_SEEN));
        ut.userSoftLockSessionId = rec.getLong(BIN_LOCK_SESSION_ID);
        ut.userSoftlockExpiry = getDate(rec.getLong(BIN_LOCK_EXPIRY));
        ut.balance = rec.getLong(BIN_BALANCE);
        ut.userDataObject = ExtraUserData.fromMap(rec.getMap(BIN_USER_DATA));

        String usage = rec.getString(BIN_USAGE);
        if (usage != null) {
            ut.userUsage = GSON.fromJson(usage, USAGE_TYPE);
        }

        String txns = rec.getString(BIN_TXNS);
        if (txns != null) {
            ut.userRecentTransactions = GSON.fromJson(txns, TXNS_TYPE);
        }

        return ut;
    }

    private static Date getDate(long epochMs) {
        if (epochMs == 0) {
            return null;
        }
        return new Date(epochMs);
    }

    public void setUserDataObject(ExtraUserData userDataObject) {
        this.userDataObject = userDataObject;
    }

    public UserUsageTable getUserUsage(long sessionId) {

        return userUsage.get(sessionId);

    }

    public void setUserUsage(UserUsageTable userUsageTable) {
        userUsage.put(userUsageTable.sessionId, userUsageTable);
    }

    public HashMap<String, UserRecentTransactions> getUserRecentTransactions() {
        return userRecentTransactions;
    }

    public boolean txHasHappened(String txId) {


        for (Map.Entry<String, UserRecentTransactions> entry : userRecentTransactions.entrySet()) {
            if (entry.getValue().userTxnId.equals(txId)) {
                return true;
            } else if (entry.getValue().txnTime.before(new Date(System.currentTimeMillis() - TX_KEEP_MS))) {
                userRecentTransactions.remove(entry.getKey());
            }
        }
        return false;
    }

    public void addUserRecentTransaction(UserRecentTransactions theUserRecentTransaction) {
        userRecentTransactions.put(theUserRecentTransaction.userTxnId, theUserRecentTransaction);
        balance += theUserRecentTransaction.spentAmount;
    }

    public long lock() {

        if (userSoftlockExpiry == null || userSoftlockExpiry.before(new Date(System.currentTimeMillis() - ReferenceData.LOCK_TIMEOUT_MS))) {
            SecureRandom secureRandom = new SecureRandom();
            userSoftLockSessionId = Math.abs(secureRandom.nextLong());
            userSoftlockExpiry = new Date(System.currentTimeMillis() + ReferenceData.LOCK_TIMEOUT_MS);
            return (userSoftLockSessionId);
        }

        return Long.MIN_VALUE;

    }

    @Override
    public String toString() {
        return "UserTable{" +
                "userId=" + userId +
                ", userDataObject='" + userDataObject.toString() + '\'' +
                ", userLastSeen=" + userLastSeen +
                ", userSoftlockExpiry=" + userSoftlockExpiry +
                ", userUsage=" + userUsage +
                ", userRecentTransactions=" + userRecentTransactions +
                ", balance=" + balance +
                '}';
    }


    public String addCredit(long extraCredit, String txnId) {

        String retstring = "";

        // Sanity Check: Has this transaction already happened?
        if (isTransactionNew(txnId)) {

            // Report credit add...
            retstring = extraCredit + ADDED_BY_TXN + txnId;
            UserRecentTransactions newTran = new UserRecentTransactions(userId, txnId, 0, extraCredit, "Add Credit");
            addUserRecentTransaction(newTran);
        } else {
            retstring = "Txn " + txnId + ALREADY_HAPPENED;
        }
        deleteOldTransactions(new Date(System.currentTimeMillis() - FIVE_MINUTES_IN_MS));

        return retstring;
    }

    int deleteOldTransactions(Date thresholdDate) {

        int deleted = 0;
        ArrayList<String> deleteList = new ArrayList<String>();

        for (Map.Entry<String, UserRecentTransactions> entry : userRecentTransactions.entrySet()) {
            if (entry.getValue().txnTime.before(thresholdDate)) {
                deleteList.add(entry.getKey());

            }
        }

        for (String key : deleteList) {
            userRecentTransactions.remove(key);
            deleted++;
        }

        return deleted;

    }

    public byte reportQuotaUsage(int unitsUsed, int unitsWanted, long inputSessionId, String txnId) {

        byte statusCode = ReferenceData.STATUS_OK;
        String decision = "none";

        // Sanity Check: Has this transaction already happened?
        if (isTransactionNew(txnId)) {

            int amountSpent = unitsUsed * -1;
            int approvedAmount = 0;


            if (unitsWanted == 0) {
                decision = "Recorded usage of " + amountSpent;
                statusCode = ReferenceData.STATUS_OK;
                UserRecentTransactions newTran = new UserRecentTransactions(userId, txnId, approvedAmount, amountSpent, decision);
                deleteReservation(inputSessionId);
                addUserRecentTransaction(newTran);
                return statusCode;
            }

            deleteReservation(inputSessionId);
            long availableCredit = getAvailableCredit() - unitsUsed;
            long amountApproved = 0;

            if (availableCredit <= 0) {

                decision = decision + "; Negative balance: " + availableCredit;
                statusCode = ReferenceData.STATUS_NO_MONEY;

            } else if (unitsWanted > availableCredit) {

                amountApproved = availableCredit;
                decision = decision + "; Allocated " + availableCredit + " units of " + unitsWanted + " asked for";
                statusCode = ReferenceData.STATUS_SOME_UNITS_ALLOCATED;

            } else {

                amountApproved = unitsWanted;
                decision = decision + "; Allocated " + unitsWanted;
                statusCode = ReferenceData.STATUS_ALL_UNITS_ALLOCATED;

            }

            updateReservation(amountApproved, inputSessionId);


            UserRecentTransactions newTran = new UserRecentTransactions(userId, txnId, approvedAmount, amountSpent, decision);
            addUserRecentTransaction(newTran);

        } else {
            statusCode = ReferenceData.STATUS_TXN_ALREADY_HAPPENED;
        }
        deleteOldTransactions(new Date(System.currentTimeMillis() - FIVE_MINUTES_IN_MS));

        return statusCode;
    }

    private void deleteReservation(long inputSessionId) {
        UserUsageTable uut = getUserUsage(inputSessionId);

        if (uut != null) {
            userUsage.remove(inputSessionId);
        }

    }

    private void updateReservation(long amountApproved, long inputSessionId) {
        UserUsageTable uut = getUserUsage(inputSessionId);

        if (uut != null) {
            uut.setAllocatedAmount(amountApproved);
        } else {
            uut = new UserUsageTable(userId, amountApproved, inputSessionId, new Date());
            userUsage.put(inputSessionId, uut);
        }

    }

    public long getAvailableCredit() {
        long availableCredit = balance;

        for (Map.Entry<Long, UserUsageTable> entry : userUsage.entrySet()) {

            availableCredit -= entry.getValue().allocatedAmount;
        }

        return availableCredit;
    }

    private void reportFinancialEvent(long amountSpent, String txnId, String decision) {
    }

    private boolean isTransactionNew(String txnId) {
        return !userRecentTransactions.containsKey(txnId);
    }

    public int getTxCount() {
        return userRecentTransactions.size();
    }

    public int getUserUsageCount() {
        return userUsage.size();
    }

    public void unLock() {
        userSoftLockSessionId = Long.MIN_VALUE;
        userSoftlockExpiry = null;
    }

    public boolean isLockedBySomeoneElse(long lockId) {

        if (userSoftLockSessionId == Long.MIN_VALUE) {
            return false;
        }

        return userSoftLockSessionId != lockId;
    }

    public int clearSessions() {

        int sessionCount = userUsage.size();
        userUsage.clear();
        return sessionCount;
    }

    public long getUsageBalance() {

        long total = 0;
        if (userUsage != null) {
            for (Map.Entry<Long, UserUsageTable> entry : userUsage.entrySet()) {
                total += entry.getValue().allocatedAmount;
            }
        }
        return total;
    }
}
