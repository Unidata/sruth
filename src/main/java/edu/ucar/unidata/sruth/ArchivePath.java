/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A path through the archive to a regular file or directory relative to the
 * root directory of the archive.
 * <p>
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
public final class ArchivePath implements Comparable<ArchivePath>, Serializable {
    /**
     * The name separator character.
     */
    static final char               SEPARATOR_CHAR   = '/';
    /**
     * The name separator character as a string.
     */
    static final String             SEPARATOR        = "/";
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The relative pathname of the file. INVARIANT: path != null &&
     * !path.isAbsolute()
     */
    private volatile transient Path path;

    /**
     * Constructs from the absolute pathname of the file or category and the
     * absolute pathname of the root directory of the archive.
     * 
     * @param path
     *            The absolute pathname of the file or category.
     * @param rootDir
     *            The absolute pathname of the root directory of the archive.
     * @throws IllegalArgumentException
     *             if {@code !path.isAbsolute()}.
     * @throws IllegalArgumentException
     *             if {@code !rootDir.isAbsolute()}.
     * @throws IllegalArgumentException
     *             if {@code path} can't be made relative to {@code rootDir}.
     * @throws NullPointerException
     *             if {@code path == null}.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    public ArchivePath(final Path path, final Path rootDir) {
        this(rootDir.relativize(path));
        if (!rootDir.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Constructs from the relative pathname of a file or category.
     * 
     * @param path
     *            The relative pathname of the file or category.
     * @throws IllegalArgumentException
     *             if {@code path.isAbsolute()}.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    public ArchivePath(final Path path) {
        if (path.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        this.path = path;
    }

    /**
     * Constructs from the string representation of a relative pathname of a
     * file or category.
     * 
     * @param path
     *            String representation of a relative pathname of a file or
     *            category.
     * @throws IllegalArgumentException
     *             if {@code path} is an absolute pathname.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    public ArchivePath(final String path) {
        this(Paths.get(path));
    }

    /**
     * Returns the relative pathname of this instance's file or category.
     * 
     * @return The relative pathname of this instance's file or category.
     */
    public Path getPath() {
        return path;
    }

    /**
     * Returns the number of names.
     * 
     * @return The number of names.
     */
    int size() {
        return path.getNameCount();
    }

    int getNameCount() {
        return path.getNameCount();
    }

    /**
     * Returns the {@code i}th name component.
     * 
     * @param i
     *            Index of the component to return
     * @return the {@code i}th name component.
     */
    Path getName(final int i) {
        return path.getName(i);
    }

    /**
     * Equal to {@code getPath().compareTo(that.getPath())}.
     * 
     * @see Comparable#compareTo(Object)
     */
    @Override
    public int compareTo(final ArchivePath that) {
        return path.compareTo(that.path);
    }

    /**
     * Indicates if this instance includes a given instance (i.e., if the
     * pathname of the given instance starts with the pathname of this
     * instance).
     * 
     * @param archivePath
     *            The given instance.
     * @return {@code true} if and only if this instance includes the given
     *         instance.
     */
    boolean includes(final ArchivePath archivePath) {
        return archivePath.path.startsWith(path);
    }

    /**
     * Indicates if this instance matches a pattern.
     * 
     * @param pattern
     *            The pattern to be matched.
     * @return {@code true} if and only if this instance matches the pattern.
     */
    boolean matches(final Pattern pattern) {
        final Matcher matcher = pattern.matcher(path.toString());
        return matcher.matches();
    }

    /**
     * Returns a {@link Matcher} based on this instance and a pattern.
     * 
     * @param pattern
     *            The pattern.
     * @return The corresponding {@link Matcher}
     */
    Matcher matcher(final Pattern pattern) {
        return pattern.matcher(path.toString());
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
        return rootDir.resolve(path);
    }

    /**
     * Resolves a {@link Path} representation of a relative pathname against
     * this instance.
     * 
     * @param relPath
     *            The {@link Path} representation of a relative pathname.
     * @return The pathname resolved against this instance.
     */
    private ArchivePath resolve(final Path relPath) {
        return new ArchivePath(path.resolve(relPath));
    }

    /**
     * Resolves another instance against this instance.
     * 
     * @param subPath
     *            The other instance.
     * @return The other instance resolved against this instance.
     */
    ArchivePath resolve(final ArchivePath subPath) {
        return resolve(subPath.path);
    }

    /**
     * Resolves a string representation of a relative pathname against this
     * instance.
     * 
     * @param string
     *            The string representation of a relative pathname
     * @return The relative pathname resolved against this instance
     * @throws IllegalArgumentException
     *             if the pathname isn't relative
     */
    ArchivePath resolve(final String string) {
        final Path relPath = Paths.get(string);
        if (relPath.isAbsolute()) {
            throw new IllegalArgumentException(relPath.toString());
        }
        return resolve(relPath);
    }

    /**
     * Indicates if this instance starts with the pathname of another instance.
     * 
     * @param that
     *            The other instance.
     * @return {@code true} if and only if this instance starts with the
     *         pathname of the other instance.
     */
    boolean startsWith(final ArchivePath that) {
        return path.startsWith(that.path);
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
        if (!(obj instanceof ArchivePath)) {
            return false;
        }
        final ArchivePath other = (ArchivePath) obj;
        if (path == null) {
            if (other.path != null) {
                return false;
            }
        }
        else if (!path.equals(other.path)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((path == null)
                ? 0
                : path.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return path.toString();
    }

    /**
     * Serializes this instance.
     * 
     * @serialData The relative pathname is written as a {@link File}.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void writeObject(final java.io.ObjectOutputStream out)
            throws IOException {
        out.defaultWriteObject();
        out.writeObject(path.toFile());
    }

    /**
     * Deserializes this instance.
     * 
     * @throws ClassNotFoundException
     *             if the deserialized object is unknown
     * @throws InvalidObjectException
     *             if the deserialized object is {@code null}, is not a
     *             pathname, or is an absolute pathname
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void readObject(final java.io.ObjectInputStream in)
            throws ClassNotFoundException, InvalidObjectException, IOException {
        in.defaultReadObject();
        final Object obj = in.readObject();
        if (obj == null) {
            throw new InvalidObjectException("Null object");
        }
        if (!(obj instanceof File)) {
            throw new InvalidObjectException("Not a File: " + obj);
        }
        path = ((File) obj).toPath();
        if (path.isAbsolute()) {
            throw new InvalidObjectException("Absolute pathname: " + path);
        }
    }
}
