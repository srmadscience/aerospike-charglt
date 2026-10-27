/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.aerospikecharglt.documents;

import com.google.gson.Gson;

import java.util.Date;

/**
 * create table user_usage_table
 * (userid bigint not null
 * ,allocated_amount bigint not null
 * ,sessionid bigint  not null
 * ,lastdate timestamp not null
 * ,primary key (userid, sessionid))
 * USING TTL 180 MINUTES ON COLUMN lastdate;
 */
public class UserUsageTable {

    public long userId;
    public long allocatedAmount;
    public long sessionId;
    public Date lastDate;


    public UserUsageTable() {
    }

    public UserUsageTable(long userId, long allocatedAmount, long sessionId, Date lastDate) {
        this.userId = userId;
        this.allocatedAmount = allocatedAmount;
        this.sessionId = sessionId;
        this.lastDate = lastDate;
    }

    public static UserUsageTable fromJson(Gson gson, String document) {
        return new Gson().fromJson(document, UserUsageTable.class);
    }

    public void setAllocatedAmount(long allocatedAmount) {
        this.allocatedAmount = allocatedAmount;
        lastDate = new Date();
    }

    @Override
    public String toString() {
        return "UserUsageTable{" +
                "userId=" + userId +
                ", allocatedAmount=" + allocatedAmount +
                ", sessionId=" + sessionId +
                ", lastDate=" + lastDate +
                '}';
    }
}
