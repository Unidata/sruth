/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

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
    private final SortedSet<FileId> archivePaths     = new TreeSet<FileId>();

    /**
     * Indicates if this instance is empty or not.
     * 
     * @return {@code true} if and only if this instance is empty.
     */
    synchronized boolean isEmpty() {
        return archivePaths.size() == 0;
    }

    /**
     * Returns the number of file or category specifications in this instance.
     * 
     * @return The number of file or category specifications.
     */
    synchronized int size() {
        return archivePaths.size();
    }

    /**
     * Removes all elements of this instance.
     */
    synchronized void clear() {
        archivePaths.clear();
    }

    /**
     * Adds the specification of a file.
     * 
     * @param fileId
     *            The identifier of the file.
     */
    synchronized void add(final FileId fileId) {
        archivePaths.add(fileId);
    }

    @Override
    public Iterator<FileId> iterator() {
        return archivePaths.iterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FileSetSpec [archivePaths=" + archivePaths + "]";
    }

    private Object readResolve() throws InvalidObjectException {
        final FileSetSpec set = new FileSetSpec();
        for (final FileId spec : archivePaths) {
            set.add(spec);
        }
        return set;
    }
}
