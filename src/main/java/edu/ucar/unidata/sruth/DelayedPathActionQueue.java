/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

/**
 * Acts upon pathnames that are descendants of a root-directory according to
 * their time-delay.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class DelayedPathActionQueue {
    /**
     * The action to be performed on pathnames from the queue.
     */
    static abstract class Action {
        /**
         * Performs an action on a pathname.
         * 
         * @param path
         *            The pathname to be acted upon.
         * @throws IOException
         *             if an I/O error occurs.
         */
        abstract void act(Path path) throws IOException;
    }

    /**
     * The logger for this class.
     */
    private static Logger                       logger         = Util.getLogger();
    /**
     * The pathname/time-delay queue.
     */
    private final PathDelayQueue                queue;
    /**
     * The number of acted-upon files.
     */
    private final AtomicLong                    actedUponCount = new AtomicLong(
                                                                       0);
    /**
     * The root-directory.
     */
    private final Path                          rootDir;
    /**
     * The thread that's executing this instance.
     */
    private final Thread                        thread;
    /**
     * The exception that caused this instance to fail.
     */
    private IOException                         exception      = null;
    /**
     * The global thread index.
     */
    private static AtomicInteger                threadIndex    = new AtomicInteger(
                                                                       0);
    /**
     * The action to be performed on pathnames from the queue.
     */
    private final DelayedPathActionQueue.Action action;

    /**
     * Constructs from the root-directory and the pathname/time-delay queue.
     * Starts running in a new thread.
     * 
     * @param rootDir
     *            The root-directory.
     * @param queue
     *            The pathname/time-delay queue.
     * @param action
     *            The action to be performed on pathnames from the queue.
     * @throws NullPointerException
     *             if {@code rootDir == null || queue == null || action == null}
     *             .
     */
    DelayedPathActionQueue(final Path rootDir, final PathDelayQueue queue,
            final DelayedPathActionQueue.Action action) {
        if (rootDir == null || queue == null || action == null) {
            throw new NullPointerException();
        }
        this.rootDir = rootDir;
        this.queue = queue;
        this.action = action;
        thread = new Thread("DelayedPathActionQueue-"
                + threadIndex.getAndIncrement()) {
            @Override
            public void run() {
                try {
                    DelayedPathActionQueue.this.run();
                }
                catch (final InterruptedException e) {
                    logger.trace("Interrupted: {}", this);
                }
                catch (final IOException e) {
                    if (!isInterrupted()) {
                        synchronized (DelayedPathActionQueue.this) {
                            exception = e;
                        }
                    }
                }
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + " [rootDir=" + rootDir
                        + ", queue=" + queue + ", action=" + action + "]";
            }
        };
        thread.start();
    }

    /**
     * Executes this instance. Doesn't return. The following actions are
     * repeatedly executed: 1) the head of the queue is retrieved (but not
     * removed); 2) the associated pathname is acted upon; and 3) the associated
     * entry is removed from the queue. Thus, the queue might contain an already
     * acted-upon entry if, for example, it is implemented using a persistent
     * file and a power failure occurs.
     * <p>
     * The ancestor directories of an acted-upon pathname are deleted when they
     * become empty.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void run() throws InterruptedException, IOException {
        for (;;) {
            final Path path = queue.peek();
            action.act(path);
            actedUponCount.incrementAndGet();
            final Path removedPath = queue.remove();
            assert path.equals(removedPath) : "Different paths: " + path
                    + " != " + removedPath;
        }
    }

    /**
     * Accepts a pathname for acting upon after a time-delay. If the appropriate
     * time is not in the future, then the file is immediately acted-upon.
     * 
     * @param path
     *            Pathname of the file relative to the root-directory.
     * @param time
     *            When the file should be acted-upon in milliseconds from now.
     * @throws IllegalArgumentException
     *             if the path isn't a descendant of the root-directory.
     * @throws IOException
     *             if an I/O error occurred or occurs.
     * @see #call()
     */
    synchronized void actUponEventurally(final Path path, final long time)
            throws IOException {
        if (exception != null) {
            throw exception;
        }
        if (!path.startsWith(rootDir)) {
            throw new IllegalArgumentException(
                    "Path not descendant of root-directory: path=\"" + path
                            + "\", rootDir=\"" + rootDir + "\"");
        }
        if (time <= 0) {
            action.act(path);
            actedUponCount.incrementAndGet();
        }
        else {
            queue.add(path, System.currentTimeMillis() + time);
        }
    }

    /**
     * Returns the number of pathnames that have not yet been acted upon.
     * 
     * @return The number of pathnames that have not yet been acted upon.
     */
    int getPendingCount() {
        return queue.size();
    }

    /**
     * Returns the number of acted-upon files.
     * 
     * @return The number of acted-upon files.
     */
    long getActedUponCount() {
        return actedUponCount.get();
    }

    /**
     * Waits until the queue is empty.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void waitUntilEmpty() throws InterruptedException {
        queue.waitUntilEmpty();
    }

    /**
     * Stops this instance. Idempotent.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void stop() throws InterruptedException, IOException {
        if (thread.isAlive()) {
            try {
                queue.close();
            }
            finally {
                thread.interrupt();
                thread.join();
            }
        }
    }
}
