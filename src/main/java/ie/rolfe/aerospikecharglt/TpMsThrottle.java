/*
 * Copyright (C) 2026 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.aerospikecharglt;

/**
 * Limits a single thread to tpMs requests per millisecond. Not thread safe - each thread needs its own.
 */
public class TpMsThrottle {

    final int tpMs;
    final int extraMs;
    long currentMs = System.currentTimeMillis();
    int tpThisMs = 0;

    /**
     * @param tpMs    requests per millisecond
     * @param extraMs an arbitrary extra delay each time we run out of requests for this ms
     */
    public TpMsThrottle(int tpMs, int extraMs) {
        this.tpMs = tpMs;
        this.extraMs = extraMs;
    }

    /**
     * Call before each request. Returns once there's capacity for it.
     */
    public void await() throws InterruptedException {

        if (++tpThisMs > tpMs) {

            while (currentMs == System.currentTimeMillis()) {
                Thread.sleep(0, 50000);
            }

            if (extraMs > 0) {
                Thread.sleep(extraMs);
            }

            currentMs = System.currentTimeMillis();
            tpThisMs = 1;
        }
    }
}
