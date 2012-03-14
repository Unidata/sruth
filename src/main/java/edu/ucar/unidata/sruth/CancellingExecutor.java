package edu.ucar.unidata.sruth;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private final Set<UninterruptibleTask<?>> tasks = Collections
                                                             .synchronizedSet(new HashSet<UninterruptibleTask<?>>());

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
        if (callable instanceof UninterruptibleTask<?>) {
            final UninterruptibleTask<T> task = (UninterruptibleTask<T>) callable;
            tasks.add(task);

            return new FutureTask<T>(task) {
                @Override
                public boolean cancel(final boolean mayInterruptIfRunning) {
                    /*
                     * {@link FutureTask#cancel(boolean)} called before {@link
                     * UninterruptibleTask#cancel()} so that {@code catch}
                     * blocks in the {@link UninterruptibleTask} can call
                     * {@link Thread#isInterrupted()} meaningfully.
                     */
                    final boolean wasCancelled = super
                            .cancel(mayInterruptIfRunning);
                    if (mayInterruptIfRunning) {
                        task.cancel();
                    }
                    return wasCancelled;
                }

                @Override
                protected void done() {
                    tasks.remove(task);
                }
            };
        }
        return super.newTaskFor(callable);
    }

    @Override
    public List<Runnable> shutdownNow() {
        synchronized (tasks) {
            for (final UninterruptibleTask<?> task : tasks) {
                task.cancel();
            }
        }
        return super.shutdownNow();
    }
}