package edu.ucar.unidata.sruth;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jcip.annotations.ThreadSafe;

/**
 * A task that is sometimes blocked in an uninterruptible state (such as reading
 * from a socket) and, consequently, needs to be explicitly stopped (such as by
 * closing the socket).
 * <p>
 * Concrete subclasses must be thread-safe so that their instances can be
 * successfully stopped.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
abstract class UninterruptibleTask<T> implements Callable<T> {
    /**
     * The cancellation flag.
     */
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    /**
     * Cancels this task. Sets the cancellation flag and then calls
     * {@link #stop()}.
     */
    void cancel() {
        isCancelled.set(true);
        stop();
    }

    /**
     * Indicates whether or not {@link #cancel()} has been called.
     * 
     * @return {@code true} if and only if {@link #cancel()} has been called.
     */
    final boolean isCancelled() {
        return isCancelled.get();
    }

    /**
     * Does whatever's necessary to stop the task (by closing a socket, for
     * example). Idempotent. Should only be called by {@link #cancel()}.
     */
    protected abstract void stop();
}