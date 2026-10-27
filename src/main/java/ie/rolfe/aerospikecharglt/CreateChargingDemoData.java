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

import com.aerospike.client.IAerospikeClient;


import com.google.gson.Gson;
import ie.rolfe.aerospikecharglt.documents.ExtraUserData;

import java.util.Arrays;
import java.util.Random;

public class CreateChargingDemoData extends BaseChargingDemo {


    /**
     * @param args
     */
    public static void main(String[] args) {

        Gson gson = new Gson();
        Random r = new Random();

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 4) {
            msg("Usage: hostnames recordcount tpms  maxinitialcredit  ");
            System.exit(1);
        }

        // Comma delimited list of host[:port]...
        String hostlist = args[0];

        // How many users
        int userCount = Integer.parseInt(args[1]);

        // Target transactions per millisecond.
        int tpMs = Integer.parseInt(args[2]);

        // How long our arbitrary JSON payload will be.
        int loblength = 120;
        final ExtraUserData ourJson = getExtraUserDataAsObject(loblength, gson, r);

        // Default credit users are 'born' with
        int initialCredit = Integer.parseInt(args[3]);


        try {


            try (IAerospikeClient aerospikeClient = connect(hostlist)) {
                msg("Connected to " + aerospikeClient.getNodes().length + " node(s)");
                upsertAllUsers(userCount, tpMs, ourJson, initialCredit, aerospikeClient);
            }
            msg("Closing connection...");

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

}
