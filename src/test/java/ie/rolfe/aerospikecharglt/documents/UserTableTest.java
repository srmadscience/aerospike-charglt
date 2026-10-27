/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.rolfe.aerospikecharglt.documents;

import com.google.gson.Gson;
import ie.rolfe.aerospikecharglt.ReferenceData;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import static ie.rolfe.aerospikecharglt.BaseChargingDemo.getExtraUserDataAsObject;
import static ie.rolfe.aerospikecharglt.documents.UserTable.FIVE_MINUTES_IN_MS;
import static org.junit.jupiter.api.Assertions.*;

class UserTableTest {

    private static final long EXTRA_CREDIT = 300;
    final long SESSION_ID = 42;
    final long NOT_A_SESSION_ID = 43;
    final long INITIAL_CREDIT = 1000;
    final long INITIAL_ALLOCATED = 10;
    final long NEXT_ALLOCATED = 10;
    final long USER_ID = 1;
    final java.util.Date INITIAL_DATE = new Date();

    public UserTable getBasicTable() {
        Gson g = new Gson();
        Random r = new Random();
        ExtraUserData eud = getExtraUserDataAsObject(100, g, r);
        UserTable ut = UserTable.getUserTable(eud, INITIAL_CREDIT, USER_ID, INITIAL_DATE.getTime());

        UserUsageTable uu = new UserUsageTable(USER_ID, INITIAL_ALLOCATED, SESSION_ID, INITIAL_DATE);
        ut.setUserUsage(uu);

        return ut;
    }

    @Test
    void getUserTable() {

        Gson g = new Gson();
        Random r = new Random();
        ExtraUserData eud = getExtraUserDataAsObject(100, g, r);
        UserTable ut = UserTable.getUserTable(eud, INITIAL_CREDIT, USER_ID, INITIAL_DATE.getTime());

        assertEquals(ut.getAvailableCredit(), INITIAL_CREDIT);
    }

    @Test
    void getUserUsage() {

        UserTable ut = getBasicTable();
        assertEquals(ut.getUserUsage(SESSION_ID).allocatedAmount, INITIAL_ALLOCATED);

    }

    @Test
    void setUserUsage() {

        UserTable ut = getBasicTable();
        UserUsageTable bar = ut.getUserUsage(NOT_A_SESSION_ID);

        assertNull(bar);

        bar = ut.getUserUsage(SESSION_ID);
        bar.setAllocatedAmount(NEXT_ALLOCATED);
        ut.setUserUsage(bar);

        assertEquals(ut.getAvailableCredit(), INITIAL_CREDIT - NEXT_ALLOCATED);

    }


    @Test
    void txHasHappened() {

        UserTable t = getBasicTable();

        assertTrue(t.txHasHappened("Create_" + USER_ID));
        assertFalse(t.txHasHappened("RANDOM" + USER_ID));


    }

    @Test
    void addCredit() {

        UserTable t = getBasicTable();

        String testTxn = "Test1";

        String add1 = t.addCredit(EXTRA_CREDIT, testTxn);
        String expected = EXTRA_CREDIT + UserTable.ADDED_BY_TXN + testTxn;
        assertEquals(add1, expected);

        String add2 = t.addCredit(EXTRA_CREDIT, testTxn);
        expected = "Txn " + testTxn + UserTable.ALREADY_HAPPENED;
        assertEquals(add2, expected);

        assertEquals(t.getAvailableCredit(), INITIAL_CREDIT + EXTRA_CREDIT - INITIAL_ALLOCATED);


    }

    @Test
    void deleteOldTransactions() {

        UserTable t = getBasicTable();

        String testTxn = "TestOld";

        String add1 = t.addCredit(EXTRA_CREDIT, testTxn + "_2");
        String expected = EXTRA_CREDIT + UserTable.ADDED_BY_TXN + testTxn + "_2";
        assertEquals(add1, expected);


        String add2 = t.addCredit(EXTRA_CREDIT, testTxn + "_3");
        expected = EXTRA_CREDIT + UserTable.ADDED_BY_TXN + testTxn + "_3";
        assertEquals(add2, expected);

        assertEquals(3, t.getTxCount());

// Fix date of oldest...
        HashMap<String, UserRecentTransactions> userRecentTransactions = t.getUserRecentTransactions();
        userRecentTransactions.get(testTxn + "_2").txnTime = new Date(System.currentTimeMillis() - (FIVE_MINUTES_IN_MS + 1));

        String add3 = t.addCredit(EXTRA_CREDIT, testTxn + "_4");
        expected = EXTRA_CREDIT + UserTable.ADDED_BY_TXN + testTxn + "_4";
        assertEquals(add3, expected);

        // Number of records should still be 3...
        assertEquals(3, t.getTxCount());


    }

    @Test
    void reportQuotaUsageSmoketest() {

        // Smoke Test
        String testTxn = "TestRQU";

        int used = 0;
        int wanted = 0;
        long expectedCredit = INITIAL_CREDIT - INITIAL_ALLOCATED;

        UserTable t = getBasicTable();

        byte resultCode = t.reportQuotaUsage(used, wanted, SESSION_ID, testTxn);

        assertEquals(ReferenceData.STATUS_OK, resultCode);
        assertEquals(t.getAvailableCredit(), INITIAL_CREDIT);


    }

    @Test
    void reportQuotaUsageZeroWanted() {

        // Smoke Test
        String testTxn = "TestRQU";

        int used = 100;
        int wanted = 0;
        long expectedCredit = INITIAL_CREDIT - INITIAL_ALLOCATED;

        UserTable t = getBasicTable();

        byte resultCode = t.reportQuotaUsage(used, wanted, SESSION_ID, testTxn);

        assertEquals(ReferenceData.STATUS_OK, resultCode);
        assertEquals(t.getAvailableCredit(), INITIAL_CREDIT - used);


    }

    @Test
    void reportQuotaUsageAllAllocated() {

        // Smoke Test
        String testTxn = "TestRQU";

        int used = 100;
        int wanted = 10;
        long expectedCredit = INITIAL_CREDIT - INITIAL_ALLOCATED;

        UserTable t = getBasicTable();

        byte resultCode = t.reportQuotaUsage(used, wanted, SESSION_ID, testTxn);

        assertEquals(ReferenceData.STATUS_ALL_UNITS_ALLOCATED, resultCode);
        assertEquals(t.getAvailableCredit(), INITIAL_CREDIT - used - wanted);


    }

    @Test
    void reportQuotaUsageSomeAllocated() {

        // Smoke Test
        String testTxn = "TestRQU";

        int used = 100;
        long wanted = INITIAL_CREDIT - INITIAL_ALLOCATED + 1;
        long expectedCredit = INITIAL_CREDIT - INITIAL_ALLOCATED;

        UserTable t = getBasicTable();

        byte resultCode = t.reportQuotaUsage(used, (int) wanted, SESSION_ID, testTxn);

        assertEquals(ReferenceData.STATUS_SOME_UNITS_ALLOCATED, resultCode);
        assertEquals(0, t.getAvailableCredit());


    }

    @Test
    void reportQuotaUsageNoneAllocated() {

        String testTxn = "TestRQU";


        UserTable t = getBasicTable();

        t.reportQuotaUsage((int) INITIAL_CREDIT, 0, SESSION_ID, testTxn + "_1");
        byte resultCode = t.reportQuotaUsage(0, 1, SESSION_ID, testTxn + "_2");

        assertEquals(ReferenceData.STATUS_NO_MONEY, resultCode);
        assertEquals(0, t.getAvailableCredit());


    }

    @Test
    void reportQuotaUsageAllAllocatedManySessions() {

        // Smoke Test
        String testTxn = "TestRQU";

        int used = 10;
        int wanted = 10;
        long expectedCredit = INITIAL_CREDIT - INITIAL_ALLOCATED;

        UserTable t = getBasicTable();

        for (int i = 1; i < 20; i++) {

            byte resultCode = t.reportQuotaUsage(0, wanted, SESSION_ID + i, testTxn + "_" + i);

            assertEquals(ReferenceData.STATUS_ALL_UNITS_ALLOCATED, resultCode);
            assertEquals(t.getAvailableCredit(), INITIAL_CREDIT - 10 - (i * (wanted)));
        }

    }


}