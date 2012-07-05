/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;

/**
 * The time associated with a file in the archive. This class exists because the
 * temporal resolution of the native {@link FileTime} can vary between
 * platforms.
 * <p>
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
@Immutable
final class ArchiveTime implements Serializable, Comparable<ArchiveTime> {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID  = 1L;
    /**
     * The timestamp format for the {@link #toString()} method.
     */
    private static final DateFormat dateFormat        = new SimpleDateFormat(
                                                              "yyyy-MM-dd'T'HH:mm:ssZ");

    /**
     * An archive time that should be older than that of any file.
     */
    static final ArchiveTime        BEGINNING_OF_TIME = new ArchiveTime(
                                                              FileTime.fromMillis(Long.MIN_VALUE));
    /**
     * The time to associate with the file in milliseconds since the epoch.
     */
    @GuardedBy("this")
    private final Long              time;

    /**
     * Constructs using the current time.
     */
    ArchiveTime() {
        time = round(System.currentTimeMillis());
    }

    /**
     * Constructs from a native file-time.
     * 
     * @throws NullPointerException
     *             if {@code fileTime == null}.
     */
    ArchiveTime(final FileTime fileTime) {
        final long millis = fileTime.toMillis();
        time = round(millis);
    }

    /**
     * Constructs from an existing file.
     * 
     * @param path
     *            Pathname of the existing file.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code path == null}.
     * @throws NoSuchFileException
     *             if the file doesn't exist.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ArchiveTime(final Path path) throws NoSuchFileException, IOException {
        this(Files.readAttributes(path, BasicFileAttributes.class));
    }

    /**
     * Constructs from attributes for a file.
     * 
     * @param attributes
     *            The attributes of the file.
     * @throws NullPointerException
     *             if {@code attributes == null}.
     */
    ArchiveTime(final BasicFileAttributes attributes) {
        this(attributes.lastModifiedTime());
    }

    /**
     * Rounds a given time to the temporal resolution of this class.
     * 
     * @param time
     *            The given time in milliseconds since the epoch.
     * @return the given time rounded to the temporal resolution of this class.
     */
    private static long round(final long millis) {
        return ((millis + 500) / 1000) * 1000; // one-second resolution
    }

    /**
     * Sets the time associated with an existing file to that of this instance.
     * 
     * @param path
     *            The pathname of the file.
     * @param time
     *            The time to associate with the file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void setTime(final Path path) throws IOException {
        // Keep consistent with {@link #ArchiveTime(BasicFileAttributes
        // attributes)}
        final FileTime fileTime = FileTime.fromMillis(time);
        Files.setAttribute(path, "lastModifiedTime", fileTime);
    }

    /**
     * Adjusts the time of an existing file to be consonant with the temporal
     * resolution of this class.
     * 
     * @param path
     *            The absolute pathname of the file.
     * @param attributes
     *            The attributes of the file. It is the client's responsibility
     *            to ensure that these attributes are those of the file.
     * @throws IllegalArgumentException
     *             if the path isn't absolute.
     * @throws IOException
     *             if an I/O error occurs.
     */
    static void adjustTime(final Path path, final BasicFileAttributes attributes)
            throws IOException {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        final ArchiveTime archiveTime = new ArchiveTime(attributes);
        archiveTime.setTime(path);
    }

    /**
     * Adjusts the time of an existing file to be consonant with the temporal
     * resolution of this class.
     * 
     * @param path
     *            The absolute pathname of the file.
     * @throws IllegalArgumentException
     *             if the path isn't absolute.
     * @throws IOException
     *             if an I/O error occurs.
     */
    static void adjustTime(final Path path) throws IOException {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        final ArchiveTime archiveTime = new ArchiveTime(path);
        archiveTime.setTime(path);
    }

    @Override
    public int compareTo(final ArchiveTime that) {
        return Long.compare(time, that.time);
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ArchiveTime other = (ArchiveTime) obj;
        return compareTo(other) == 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Long.valueOf(time).hashCode();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final Date date = new Date(time);
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }
}
