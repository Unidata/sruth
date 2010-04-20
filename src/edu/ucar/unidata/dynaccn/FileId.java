/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.InvalidObjectException;
import java.io.Serializable;
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
    private static final long serialVersionUID = 1L;
    /**
     * The abstract relative pathname of the file.
     */
    private final File        relFile;

    /**
     * Constructs from the relative pathname of the file.
     * 
     * @param relFile
     *            The relative pathname of the file.
     * @throws IllegalArgumentException
     *             if {@code relFile.isAbsolute()}.
     * @throws NullPointerException
     *             if {@code relFile == null}.
     */
    FileId(final File relFile) {
        if (relFile.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        this.relFile = relFile;
    }

    /**
     * Returns the abstract relative pathname associated with this instance.
     * 
     * @return The associated abstract relative pathname.
     */
    File getFile() {
        return relFile;
    }

    /**
     * Returns the abstract absolute pathname of this instance's file resolved
     * against the abstract absolute pathname of a given directory.
     * 
     * @param dir
     *            The abstract absolute pathname of the directory against which
     *            to resolve the abstract relative pathname associated with this
     *            instance.
     * @throws IllegalArgumentException
     *             if {@code !dirPath.isAbsolute()}.
     * @throws NullPointerException
     *             if {@code dirPath == null}.
     */
    File getFile(final File dir) {
        if (!dir.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        return new File(dir, relFile.getPath());
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
                ? relFile.getPath()
                : null;
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
        if (relFile == null) {
            if (other.relFile != null) {
                return false;
            }
        }
        else if (!relFile.equals(other.relFile)) {
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
        result = prime * result + ((relFile == null)
                ? 0
                : relFile.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{relFile=" + relFile + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new FileId(relFile);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }

    /**
     * Returns the map of attributes and their values.
     * 
     * @return The map of attributes and their values.
     */
    Map<Attribute, Object> getAttributeMap() {
        return Collections.singletonMap(new Attribute("name"), (Object) relFile
                .getPath());
    }
}
