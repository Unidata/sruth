/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages tasks.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class TaskManager<T> {
    /**
     * The logger to use.
     */
    private static Logger                      logger  = LoggerFactory
                                                               .getLogger(TaskManager.class);
    /**
     * The executor completion service.
     */
    private final ExecutorCompletionService<T> completionService;
    /**
     * The list of task futures.
     */
    private final List<Future<T>>              futures = new LinkedList<Future<T>>();
    /**
     * Whether or not this instance will reject further task submissions.
     * 
     * @see {@link #cancel()}.
     */
    private volatile boolean                   isClosed;

    /**
     * Constructs from an executor service.
     * 
     * @param executorService
     *            The executor service.
     * @throws NullPointerException
     *             if {@code executorService == null}.
     */
    TaskManager(final ExecutorService executorService) {
        completionService = new ExecutorCompletionService<T>(executorService);
    }

    /**
     * Submits a task.
     * 
     * @param task
     *            The task to submit.
     * @return {@code true} if and only if the task was accepted for execution.
     * @throws RejectedExecutionException
     *             if the executor service rejects the task.
     * @see {@link #cancel()}.
     */
    synchronized boolean submit(final Callable<T> task) {
        if (isClosed) {
            return false;
        }
        final Future<T> future = completionService.submit(task);
        futures.add(future);
        return true;
    }

    /**
     * Returns the future of the next completed task or {@code null} if there
     * are no more tasks. Waits for a task to complete if one isn't already
     * available.
     * 
     * @return The next completed task future or {@code null}.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    Future<T> next() throws InterruptedException {
        synchronized (this) {
            if (0 == futures.size()) {
                return null;
            }
        }
        final Future<T> future = completionService.take();
        synchronized (this) {
            futures.remove(future);
        }
        return future;
    }

    /**
     * Waits for all tasks to complete. Ignores task results. If a task
     * completes abnormally, then the cause is logged if it's not due to a
     * cancellation and all remaining tasks are canceled. If the current thread
     * is interrupted, then all submitted tasks are canceled before returning.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @see {@link #cancel()}.
     */
    void waitUpon() throws InterruptedException {
        try {
            for (Future<T> future = next(); null != future; future = next()) {
                try {
                    future.get();
                }
                catch (final CancellationException ignored) {
                }
                catch (final ExecutionException e) {
                    logger.error("Abnormal task completion", e.getCause());
                    cancel();
                }
            }
        }
        catch (final InterruptedException e) {
            cancel();
            throw e;
        }
    }

    /**
     * Cancels all submitted tasks by interrupting all task threads. May be
     * called even if another thread is executing {@link #waitUpon()}. Causes
     * rejection of all further task submissions.
     * 
     * @see {@link #waitUpon()}.
     * @see {@link #submit(Callable)}.
     */
    synchronized void cancel() {
        for (final Future<T> future : futures) {
            future.cancel(true);
        }
        isClosed = true;
    }
}
