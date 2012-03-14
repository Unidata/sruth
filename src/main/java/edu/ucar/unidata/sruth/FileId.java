/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.jcip.annotations.Immutable;

/**
 * Unique, logical identifier for a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
@Immutable
final class FileId implements Serializable, Comparable<FileId> {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The pathname of the file in the archive.
     * 
     * @serial
     */
    private final ArchivePath path;
    /**
     * The time associated with the file.
     * 
     * @serial
     */
    private final ArchiveTime time;

    /**
     * Constructs from an archive pathname and an archive time.
     * 
     * @param path
     *            The pathname in the archive.
     * @param time
     *            The associated archive time.
     * @throws NullPointerException
     *             if {@code path == null}.
     * @throws NullPointerException
     *             if {@code time == null}.
     */
    FileId(final ArchivePath path, final ArchiveTime time) {
        if (path == null) {
            throw new NullPointerException();
        }
        if (time == null) {
            throw new NullPointerException();
        }
        this.path = path;
        this.time = time;
    }

    /**
     * Constructs from an archive pathname. The archive time will be
     * {@link ArchiveTime.BEGINNING_OF_TIME}.
     * 
     * @param archivePath
     *            The archive pathname.
     */
    FileId(final ArchivePath archivePath) {
        this(archivePath, ArchiveTime.BEGINNING_OF_TIME);
    }

    /**
     * Returns an instance constructed from an existing file.
     * 
     * @param path
     *            Absolute pathname of the file.
     * @param rootDir
     *            Absolute pathname of the root-directory of the archive.
     * @throws IllegalArgumentException
     *             if {@code !path.isAbsolute()}
     * @throws IllegalArgumentException
     *             if {@code !rootDir.isAbsolute()}
     * @throws IOException
     *             if an I/O error occurs.
     */
    static FileId getInstance(final Path path, final Path rootDir)
            throws IOException {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException(path.toString());
        }
        if (!rootDir.isAbsolute()) {
            throw new IllegalArgumentException(rootDir.toString());
        }
        final ArchivePath archivePath = new ArchivePath(path, rootDir);
        final ArchiveTime time = new ArchiveTime(path);
        return new FileId(archivePath, time);
    }

    /**
     * Returns the archive pathname.
     * 
     * @return the archive pathname.
     */
    ArchivePath getPath() {
        return path;
    }

    /**
     * Returns the archive time of the file.
     * 
     * @return the archive time of the file.
     */
    ArchiveTime getTime() {
        return time;
    }

    /**
     * Indicates if this instance matches a pathname pattern.
     * 
     * @param pattern
     *            The pathname pattern.
     * @return {@code true} if an only if this instance matches the pathname
     *         pattern.
     */
    boolean matches(final Pattern pattern) {
        return path.matches(pattern);
    }

    /**
     * Returns a {@link Matcher} based on this instance and a pattern.
     * 
     * @param pattern
     *            The pattern.
     * @return The corresponding {@link Matcher}.
     */
    Matcher matcher(final Pattern pattern) {
        return path.matcher(pattern);
    }

    @Override
    public int compareTo(final FileId that) {
        int cmp = path.compareTo(that.path);
        if (cmp == 0) {
            cmp = time.compareTo(that.time);
        }
        return cmp;
    }

    /**
     * Returns the absolute pathname of this instance resolved against a
     * directory.
     * 
     * @param rootDir
     *            The pathname of the directory against which to resolve this
     *            instance's pathname.
     * @return An absolute pathname corresponding to this instance resolved
     *         against the given directory.
     */
    Path getAbsolutePath(final Path rootDir) {
        return path.getAbsolutePath(rootDir);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof FileId)) {
            return false;
        }
        final FileId other = (FileId) obj;
        return compareTo(other) == 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return path.hashCode() ^ time.hashCode();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FileId [path=" + path + ", time=" + time + "]";
    }

    private Object readResolve() throws ObjectStreamException {
        try {
            return new FileId(path, time);
        }
        catch (final NullPointerException e) {
            throw (ObjectStreamException) new InvalidObjectException(
                    "Invalid argument").initCause(e);
        }
    }
}
