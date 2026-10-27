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

import org.voltdb.voltutil.stats.SafeHistogramCache;


public class UserKVState {

    public static final byte STATUS_UNLOCKED = 0;
    public static final byte STATUS_TRYING_TO_LOCK = 1;
    public static final byte STATUS_LOCKED = 2;
    public static final byte STATUS_UPDATING = 3;
    public static final byte STATUS_LOCKED_BY_SOMEONE_ELSE = 4;

    /**
     * Unique ID given to  that we use to prove that we are the owner of
     * this lock.
     */
    long lockId = Long.MIN_VALUE;

    /**
     * ID of user.
     */
    int id = 0;

    /**
     * Where we are in the update cycle..
     */
    int userState = STATUS_UNLOCKED;

    /**
     * When a transaction started, or zero if there isn't one.
     */
    long txStartMicros = 0;

    /**
     * Last time record was known to be locked by someone else...
     */
    long otherLockTimeMs = 0;

    /**
     * Times record was locked by another session
     */
    long lockedBySomeoneElseCount = 0;

    SafeHistogramCache shc;

    /**
     * Create a record for a user.
     *
     * @param id
     */
    public UserKVState(int id, SafeHistogramCache shc) {
        this.id = id;
        this.shc = shc;
        userState = STATUS_UNLOCKED;

    }

    public void setStatus(int newStatus) {
        userState = newStatus;
    }

    /**
     * Report start of transaction.
     */
    public void startTran() {

        txStartMicros = System.nanoTime() / 1000;
    }

    /**
     * @return the txInFlight
     */
    public boolean isTxInFlight() {

        return txStartMicros > 0;
    }

    public int getUserStatus() {
        return userState;
    }
//TODO
//    @Override
//    public void clientCallback(ClientResponse arg0) throws Exception {
//
//        if (arg0.getStatus() == ClientResponse.SUCCESS) {
//
//            byte statusByte = arg0.getAppStatus();
//
//            if (userState == STATUS_UNLOCKED) {
//                BaseChargingDemo.msg("UserKVState.clientCallback: got app status of " + arg0.getAppStatusString());
//            } else if (userState == STATUS_TRYING_TO_LOCK) {
//
//                shc.reportLatencyMicros(BaseChargingDemo.KV_GET, txStartMicros, BaseChargingDemo.KV_GET,
//                        BaseChargingDemo.HISTOGRAM_SIZE_MS, 1);
//
//                if (statusByte == ReferenceData.STATUS_RECORD_HAS_BEEN_SOFTLOCKED) {
//
//                    userState = STATUS_LOCKED;
//                    lockId = arg0.getAppStatusString();
//
//                } else if (statusByte == ReferenceData.STATUS_RECORD_ALREADY_SOFTLOCKED) {
//
//                    userState = STATUS_LOCKED_BY_SOMEONE_ELSE;
//                    lockId = "";
//                    lockedBySomeoneElseCount++;
//                    otherLockTimeMs = System.currentTimeMillis();
//
//                } else {
//                    userState = STATUS_UNLOCKED;
//                }
//            } else if (userState == STATUS_UPDATING) {
//
//                shc.reportLatencyMicros(BaseChargingDemo.KV_PUT, txStartMicros, BaseChargingDemo.KV_PUT,
//                        BaseChargingDemo.HISTOGRAM_SIZE_MS, 1);
//
//                lockId = "";
//                userState = STATUS_UNLOCKED;
//
//            }
//
//        } else {
//            BaseChargingDemo.msg("UserKVState.clientCallback: got status of " + arg0.getStatusString());
//        }
//
//        // End transaction
//        txStartMicros = 0;
//    }

    /**
     * @return the lockId
     */
    public long getLockId() {
        return lockId;
    }

    /**
     * @param lockId the lockId to set
     */
    public void setLockId(long lockId) {
        this.lockId = lockId;

        if (lockId == Long.MIN_VALUE) {
            userState = STATUS_UNLOCKED;
            txStartMicros = 0;
        } else {
            userState = STATUS_LOCKED;
        }
    }

    @Override
    public String toString() {
        String builder = "UserKVState [lockId=" +
                lockId +
                ", id=" +
                id +
                ", userState=" +
                userState +
                ", txStartMs=" +
                txStartMicros +
                ", lockedBySomeoneElseCount=" +
                lockedBySomeoneElseCount +
                "]";
        return builder;
    }

    /**
     * @return the lockedBySomeoneElseCount
     */
    public long getLockedBySomeoneElseCount() {
        return lockedBySomeoneElseCount;
    }

    /**
     * @return the otherLockTimeMs
     */
    public long getOtherLockTimeMs() {
        return otherLockTimeMs;
    }

    public void endTran() {
        txStartMicros = 0;
    }
}
