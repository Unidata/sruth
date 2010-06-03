/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A set of file or category specifications in which no specification is a
 * subset of another.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class FileSetSpec implements Serializable, Iterable<FileId> {
    /**
     * The serial version ID.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The set of specifications. No specification in the set is a subset of
     * another.
     */
    @GuardedBy("this")
    private final SortedSet<FileId> fileIds          = new TreeSet<FileId>();

    /**
     * Indicates if this instance is empty or not.
     * 
     * @return {@code true} if and only if this instance is empty.
     */
    synchronized boolean isEmpty() {
        return fileIds.size() == 0;
    }

    /**
     * Returns the number of file or category specifications in this instance.
     * 
     * @return The number of file or category specifications.
     */
    synchronized int size() {
        return fileIds.size();
    }

    /**
     * Removes all elements of this instance.
     */
    synchronized void clear() {
        fileIds.clear();
    }

    /**
     * Adds a specification of a file or category.
     * 
     * @param fileId
     *            The specification of a file or category.
     */
    synchronized void add(final FileId fileId) {
        removeInclusionsOf(fileId);
        fileIds.add(fileId);
    }

    /**
     * Removes all specifications that are included by a given specification.
     * 
     * @param spec
     *            The given specification.
     */
    @GuardedBy("this")
    private void removeInclusionsOf(final FileId target) {
        final SortedSet<FileId> tailSet = fileIds.tailSet(target);
        for (final Iterator<FileId> iter = tailSet.iterator(); iter.hasNext();) {
            final FileId fileId = iter.next();
            if (!target.includes(fileId)) {
                break;
            }
            iter.remove();
        }
    }

    @Override
    public Iterator<FileId> iterator() {
        return fileIds.iterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FileSetSpec [fileIds=" + fileIds + "]";
    }

    private Object readResolve() throws InvalidObjectException {
        final FileSetSpec set = new FileSetSpec();
        for (final FileId spec : fileIds) {
            set.add(spec);
        }
        return set;
    }
}
