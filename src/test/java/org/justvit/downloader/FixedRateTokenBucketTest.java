package org.justvit.downloader;

import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class FixedRateTokenBucketTest {
    private final static Queue<Integer> queue = new ConcurrentLinkedQueue<>();
    private static final int parallelism = Runtime.getRuntime().availableProcessors() * 2;
    public static final int N = 1000;
    public static final int FIXED_RATE = 50;
    public static final int DELTA_THRESHOLD = (int) ((N / FIXED_RATE) * 1.01 *1000);

    @Test
    public void testConsume() throws Exception {
        final FixedRateTokenBucket bucket = new FixedRateTokenBucket(FIXED_RATE, FIXED_RATE, 1, TimeUnit.SECONDS);
        IntStream.range(1, N).forEach(queue::add);

        final CountDownLatch latch = new CountDownLatch(parallelism);

        Runnable sucker = () -> {
            Integer a;
            do {
                long slept = bucket.consume(1);
                if (slept > 0)
                    System.out.printf("[%d]: slept for %,d ns\n", Thread.currentThread().getId(), slept);
                a = queue.poll();
                System.out.printf("[%d]: %s\n", Thread.currentThread().getId(), a);
            } while (a != null);
            latch.countDown();
            System.out.printf("[%d]: LATCH DECREASED! [%d]\n", Thread.currentThread().getId(), latch.getCount());
        };

        final ExecutorService exec = Executors.newFixedThreadPool(parallelism);

        long start = System.nanoTime();

        for (int i = 0; i < parallelism; i++) {
            exec.execute(sucker);
        }

        latch.await();
        exec.shutdown();

        final long delta = System.nanoTime() - start;
        System.out.printf("RAN FOR [%,d] ns < %,d ms\n", delta, DELTA_THRESHOLD);

        assertTrue(TimeUnit.NANOSECONDS.toMillis(delta) < DELTA_THRESHOLD);
    }
}