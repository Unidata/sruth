/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.util.concurrent.Callable;

import net.jcip.annotations.ThreadSafe;

/**
 * A wrapper-class for {@link Callable}s to allow task-specific actions to be
 * taken after the task completes.
 * <p>
 * Instances should be submitted to a {@link CancellingExecutor}.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 * @param <T>
 */
@ThreadSafe
abstract class CallableWrapper<T> extends UninterruptibleTask<T> {
    /**
     * The wrapped {@link Callable}
     */
    private final Callable<T> callable;

    /**
     * Constructs from the {@link Callable} to be wrapped.
     * 
     * @param callable
     *            The {@link Callable} to be wrapped
     * @throws NullPointerException
     *             if {@code callable == null}
     */
    CallableWrapper(final Callable<T> callable) {
        if (callable == null) {
            throw new NullPointerException();
        }
        this.callable = callable;
    }

    /**
     * Executes this instance.
     */
    public final T call() throws Exception {
        Throwable thrown = null;
        try {
            return callable.call();
        }
        catch (final Throwable t) {
            thrown = t;
        }
        finally {
            afterExecute(callable, thrown);
        }
        return null;
    }

    /**
     * Stops this instance. If the {@link Callable} is an
     * {@link UninterruptibleTask}, then its
     * {@link UninterruptibleTask#cancel()} method is called; otherwise, nothing
     * is done.
     */
    @Override
    protected void stop() {
        if (callable instanceof UninterruptibleTask) {
            ((UninterruptibleTask<T>) callable).cancel();
        }
    }

    /**
     * Performs task-specific actions after the {@link Callable} completes.
     * 
     * @param callable
     *            The task
     * @param thrown
     *            The {@Throwable} that caused the task to complete
     *            or {@code null}
     */
    protected abstract void afterExecute(Callable<T> callable, Throwable thrown);
}
