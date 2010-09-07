/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * An identifier for a file or category.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileId implements Comparable<FileId>, Serializable {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The relative pathname of the file.
     */
    private volatile transient Path path;

    /**
     * Constructs from the relative pathname of the file or category.
     * 
     * @param path
     *            The relative pathname of the file or category.
     * @throws IllegalArgumentException
     *             if {@code path.isAbsolute()}.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    FileId(final Path path) {
        if (path.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        this.path = path;
    }

    /**
     * Returns the relative pathname of this instance's file or category.
     * 
     * @return The relative pathname of this instance's file or category.
     */
    Path getPath() {
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

    /**
     * Equal to {@code getPath().compareTo(that.getPath())}.
     * 
     * @see {@link Comparable#compareTo(Object)}.
     */
    @Override
    public int compareTo(final FileId that) {
        return path.compareTo(that.path);
    }

    /**
     * Indicates if this instance includes a given instance (i.e., if the
     * pathname of the given instance starts with the pathname of this
     * instance).
     * 
     * @param fileId
     *            The given instance.
     * @return {@code true} if and only if this instance includes the given
     *         instance.
     */
    boolean includes(final FileId fileId) {
        return fileId.path.startsWith(path);
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
     * @serialData The relative pathname is written as a string.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void writeObject(final java.io.ObjectOutputStream out)
            throws IOException {
        out.defaultWriteObject();
        out.writeObject(path.toString());
    }

    /**
     * Deserializes this instance.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     * @throws ClassNotFoundException
     */
    private void readObject(final java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        path = Paths.get((String) in.readObject());
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new FileId(path);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
