package edu.ucar.unidata.dynaccn;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

/**
 * An executor service that supports the cancellation of tasks that use sockets.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
class CancellingExecutor extends ThreadPoolExecutor {
    /**
     * Constructs from {@link ThreadPoolExecutor} parameters.
     * 
     * @param corePoolSize
     *            Minimum number of threads to keep.
     * @param maximumPoolSize
     *            Maximum number of threads.
     * @param keepAliveTime
     *            How long to retain idle threads when above {@code
     *            corePoolSize}.
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
        if (callable instanceof BlockingTask<?>) {
            return ((BlockingTask<T>) callable).newTask();
        }
        else {
            return super.newTaskFor(callable);
        }
    }
}