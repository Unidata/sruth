/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.util.Iterator;
import java.util.NoSuchElementException;

import net.jcip.annotations.NotThreadSafe;

/**
 * A simple iterator.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
class SimpleIterator<T> implements Iterator<T> {
    /**
     * The next object to be returned.
     */
    private T next;

    /**
     * Constructs from the first object to be returned. Doesn't call
     * {@link #getNext()}.
     * 
     * @param first
     *            The first object to be returned or {@code null}.
     */
    protected SimpleIterator(final T first) {
        this.next = first;
    }

    /**
     * Constructs from nothing. Calls {@link #getNext()} to obtain the first
     * element.
     */
    protected SimpleIterator() {
        next = getNext();
    }

    @Override
    public final boolean hasNext() {
        return null != next;
    }

    /**
     * Returns the next element of the iteration.
     * 
     * This implementation calls {@link #getNext()} to get the object that the
     * next call to {@link #next()} should return.
     * 
     * @return The next element of the iteration.
     * @throws NoSuchElementException
     *             if there are no more elements in the iteration.
     */
    @Override
    public final T next() {
        if (null == next) {
            throw new NoSuchElementException();
        }
        final T tmp = next;
        next = getNext();
        return tmp;
    }

    /**
     * Returns the object that the next call to {@link #next()} should return.
     * Called by {@link #SimpleIterator()} and {@link #next()}. Returns {@code
     * null} when the iteration has no more elements.
     * 
     * Override this method judiciously because it will be called by {@link
     * SimpleIterator()} before the subclass is constructed -- in particular, it
     * will be called before any instance fields of the subclass are
     * initialized.
     * 
     * This implementation returns {@code null}.
     * 
     * @return The next element or {@code null}.
     */
    protected T getNext() {
        return null;
    }

    /**
     * This implementation always throws {@link UnsupportedOperationException}.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
