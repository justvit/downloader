package org.justvit.downloader;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FixedRateTokenBucket {
    private final long capacity;
    private long nTokensInBucket;
    private final long period;
    private final long tokenRate;
    private long refillTime;

    public FixedRateTokenBucket(long capacity, long tokenRate, long period, TimeUnit unit) {
        this.capacity = capacity;
        this.tokenRate = tokenRate;
        this.period = unit.toNanos(period);
    }

    public long consume(long quantity) {
        boolean slept = false;
        final long start = System.nanoTime();
        while (!tryConsume(quantity)) {
            sleep();
            slept = true;
        }
        return slept ? System.nanoTime() - start : 0;
    }

    private synchronized boolean tryConsume(long quantity) {
        if (nTokensInBucket >= quantity) {
            nTokensInBucket -= quantity;
            return true;
        }

        nTokensInBucket += Math.min(capacity, refill());
        return false;
    }

    private long refill() {
        final long now = System.nanoTime();
        if (now < refillTime) {
            return 0;
        } else {
            refillTime = now + period;
            return tokenRate;
        }
    }


    private void sleep() {
        try {
            TimeUnit.NANOSECONDS.sleep(1);
        } catch (InterruptedException ignored){ }
    }

}
