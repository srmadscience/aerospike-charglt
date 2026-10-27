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




import java.util.Arrays;

public class ChargingDemoKVStore extends BaseChargingDemo {


    /**
     * @param args
     */
    public static void main(String[] args) {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 7 && args.length != 8) {
            msg("Usage: hostnames recordcount tpms durationseconds queryseconds jsonsize deltaProportion [threads]");
            System.exit(1);
        }

        // Comma delimited list of host[:port]...
        String hostlist = args[0];
        // How many users
        int userCount = Integer.parseInt(args[1]);

        // Target transactions per millisecond.
        int tpMs = Integer.parseInt(args[2]);

        // How many threads. Defaults to the number of CPUs.
        int threadCount = getThreadCount(args, 7);

        // Runtime for TRANSACTIONS in seconds.
        int durationSeconds = Integer.parseInt(args[3]);

        // How often we do global queries...
        int globalQueryFreqSeconds = Integer.parseInt(args[4]);

        // How often we do global queries...
        int jsonsize = Integer.parseInt(args[5]);

        int deltaProportion = Integer.parseInt(args[6]);

        // Extra delay for testing really slow hardware
        int extraMs = getExtraMsIfSet();

        boolean ok = true;
        try {
            try (IAerospikeClient aerospikeClient = connect(hostlist)) {
                unlockAllRecords(aerospikeClient);
                 ok = runKVBenchmark(userCount, tpMs, durationSeconds, globalQueryFreqSeconds, jsonsize, aerospikeClient,
                        deltaProportion, extraMs, threadCount);
            }


            msg("Closing connection...");

            if (ok) {
                System.exit(0);
            }

            msg(UNABLE_TO_MEET_REQUESTED_TPS);
            System.exit(1);

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }


}
