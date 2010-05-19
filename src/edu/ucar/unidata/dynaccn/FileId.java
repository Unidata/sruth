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
import java.util.Collections;
import java.util.Map;

/**
 * An identifier for a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileId implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The relative pathname of the file.
     */
    private volatile transient Path path;

    /**
     * Constructs from the relative pathname of the file.
     * 
     * @param path
     *            The relative pathname of the file.
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
     * Returns the abstract relative pathname associated with this instance.
     * 
     * @return The associated abstract relative pathname.
     */
    Path getFile() {
        return path;
    }

    /**
     * Returns the relative pathname of this instance's file.
     * 
     * @return The relative pathname of this instance's file.
     */
    Path getPath() {
        return path;
    }

    /**
     * Returns the value of a given attribute.
     * 
     * @param attribute
     *            The attribute.
     * @return The value of the attribute in this instance or {@code null} if
     *         this instance doesn't have the attribute.
     */
    Object getAttributeValue(final Attribute attribute) {
        return (attribute.getType().equals(String.class) && attribute.getName()
                .equals("name"))
                ? path.toString()
                : null;
    }

    /**
     * Returns the map of attributes and their values.
     * 
     * @return The map of attributes and their values.
     */
    Map<Attribute, Object> getAttributeMap() {
        return Collections.singletonMap(new Attribute("name"), (Object) path
                .toString());
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
        return getClass().getSimpleName() + "{path=" + path + "}";
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
