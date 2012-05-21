package edu.ucar.unidata.sruth;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * An executor service that supports the cancellation of
 * {@link UninterruptibleTask}-s.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
class CancellingExecutor extends ThreadPoolExecutor {
    /**
     * A {@link FutureTask} with an overridden
     * {@link FutureTask#cancel(boolean)} method that calls
     * {@link UninterruptibleTask#cancel()} when appropriate.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static final class UninterruptibleFuture<T> extends FutureTask<T> {
        /**
         * The {@link UninterruptibleTask}
         */
        private final UninterruptibleTask<T> task;

        /*
         * @param callable
         */
        public UninterruptibleFuture(final UninterruptibleTask<T> task) {
            super(task);
            this.task = task;
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            /*
             * {@link FutureTask#cancel(boolean)} called before {@link
             * UninterruptibleTask#cancel()} so that {@code catch} blocks in the
             * {@link UninterruptibleTask} can call {@link
             * Thread#isInterrupted()} meaningfully.
             */
            final boolean wasCancelled = super.cancel(mayInterruptIfRunning);
            if (mayInterruptIfRunning) {
                task.cancel();
            }
            return wasCancelled;
        }
    }

    /**
     * The executing {@link UninterruptibleFuture}s that will need to be
     * cancelled when {@link #shutdownNow()} is called
     */
    @GuardedBy("itself")
    private final List<UninterruptibleFuture<?>> uninterruptibleFutures = new LinkedList<UninterruptibleFuture<?>>();

    /**
     * Constructs from {@link ThreadPoolExecutor} parameters.
     * 
     * @param corePoolSize
     *            Minimum number of threads to keep.
     * @param maximumPoolSize
     *            Maximum number of threads.
     * @param keepAliveTime
     *            How long to retain idle threads when above
     *            {@code corePoolSize}.
     * @param unit
     *            Time unit of {@code keepAliveTime}.
     * @param workQueue
     *            Queue for submitted tasks.
     */
    CancellingExecutor(final int corePoolSize, final int maximumPoolSize,
            final long keepAliveTime, final TimeUnit unit,
            final BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
        if (!(callable instanceof UninterruptibleTask<?>)) {
            return super.newTaskFor(callable);
        }
        final UninterruptibleFuture<T> uninterruptibleFuture = new UninterruptibleFuture<T>(
                (UninterruptibleTask<T>) callable);
        synchronized (uninterruptibleFutures) {
            uninterruptibleFutures.add(uninterruptibleFuture);
        }
        return uninterruptibleFuture;
    }

    @Override
    protected void beforeExecute(final Thread t, final Runnable r) {
        if (r instanceof UninterruptibleFuture<?>) {
            synchronized (uninterruptibleFutures) {
                uninterruptibleFutures.add((UninterruptibleFuture<?>) r);
            }
        }
        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(final Runnable r, final Throwable t) {
        super.afterExecute(r, t);
        if (r instanceof UninterruptibleFuture<?>) {
            synchronized (uninterruptibleFutures) {
                uninterruptibleFutures.remove(r);
            }
        }
    }

    /**
     * This implementation calls {@link ThreadPoolExecutor#shutdownNow()} and
     * also calls {@link Future#cancel(boolean)} (with the value {@code true})
     * of every submitted {@link UninterruptibleTask}.
     */
    @Override
    public List<Runnable> shutdownNow() {
        final List<Runnable> list = super.shutdownNow();
        synchronized (uninterruptibleFutures) {
            for (final UninterruptibleFuture<?> future : uninterruptibleFutures) {
                future.cancel(true);
            }
        }
        return list;
    }
}