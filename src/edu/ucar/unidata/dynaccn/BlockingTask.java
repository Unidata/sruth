package edu.ucar.unidata.dynaccn;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import net.jcip.annotations.ThreadSafe;

/**
 * A task that is sometimes blocked in an non-interruptible action (such as
 * reading from a socket) and, consequently, needs to be explicitly stopped when
 * its future is canceled.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
abstract class BlockingTask<T> implements Callable<T> {
    /**
     * Whether or not this task has been stopped.
     */
    private volatile boolean isCanceled;

    /**
     * Returns a new task future that will call {@link #stop()} if the future is
     * canceled. Designed to be called by {@link CancellingExecutor}.
     * 
     * @return The new task future.
     */
    RunnableFuture<T> newTask() {
        return new FutureTask<T>(this) {
            @Override
            public boolean cancel(final boolean mayInterruptIfRunning) {
                isCanceled = true;
                stop();
                return super.cancel(mayInterruptIfRunning);
            }
        };
    }

    /**
     * Stops the task. Will be called by the object returned by
     * {@link #newTask()}. May be called by other objects to stop the task.
     * After this method is called, {@link #isCanceled()} will always return
     * true.
     */
    protected abstract void stop();

    /**
     * Indicates if {@link #stop()} has been called.
     * 
     * @return {@code true} if and only if this task has been stopped.
     */
    protected final boolean isCanceled() {
        return isCanceled;
    }
}