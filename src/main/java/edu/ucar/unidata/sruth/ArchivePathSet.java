/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.Serializable;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A set of archive pathnames in which no pathname is a subset of another.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ArchivePathSet implements Serializable, Iterable<ArchivePath> {
    /**
     * The serial version ID.
     */
    private static final long            serialVersionUID = 1L;
    /**
     * The set of pathnames. No pathnames in the set is a subset of another. NB:
     * The set uses natural ordering; therefore, it will not contain a null
     * element.
     */
    @GuardedBy("this")
    private final SortedSet<ArchivePath> archivePaths     = new TreeSet<ArchivePath>();

    /**
     * Indicates if this instance is empty or not.
     * 
     * @return {@code true} if and only if this instance is empty.
     */
    synchronized boolean isEmpty() {
        return archivePaths.size() == 0;
    }

    /**
     * Returns the number of pathnames in this instance.
     * 
     * @return The number of pathnames specifications.
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
     * @param archivePath
     *            The archive-pathname of the file.
     * @throws NullPointerException
     *             if {@code archivePath == null}
     */
    synchronized void add(final ArchivePath archivePath) {
        /*
         * The following will throw a NullPointerException if archivePath is
         * null.
         */
        archivePaths.add(archivePath);
    }

    @Override
    public Iterator<ArchivePath> iterator() {
        return archivePaths.iterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ArchivePathSet [archivePaths=" + archivePaths + "]";
    }
}
