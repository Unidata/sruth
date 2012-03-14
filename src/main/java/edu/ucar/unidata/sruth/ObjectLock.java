/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A container for a single object whose content can be immediately set but that
 * blocks when the content is being retrieved. Such a class is useful in a
 * producer/consumer context when only the latest produced object is important.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ObjectLock<T> {
    /**
     * The content.
     */
    @GuardedBy("this")
    private T       content;
    /**
     * Whether or not the lock is empty.
     */
    @GuardedBy("this")
    private boolean isEmpty = true;

    /**
     * Puts an object into the lock. Returns the previous content.
     * 
     * @param next
     *            The object to put into the lock. May be {@code null}.
     * @return the previous object in the lock. May be {@code null}.
     */
    synchronized T put(final T next) {
        final T prev = content;
        content = next;
        isEmpty = false;
        notify();
        return prev;
    }

    /**
     * Returns the object in the lock -- making it empty. Blocks until the lock
     * is not empty.
     * 
     * @return The current content of the lock.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    synchronized T take() throws InterruptedException {
        while (isEmpty) {
            wait();
        }
        isEmpty = true;
        return content;
    }

    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + " [content=" + content + "]";
    }
}
