/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.InvalidObjectException;
import java.io.Serializable;

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
     * The pathname of the file.
     */
    private final File        path;

    /**
     * Constructs from the pathname of the file.
     * 
     * @param path
     *            The pathname of the file.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    FileId(final File path) {
        if (null == path) {
            throw new NullPointerException();
        }
        this.path = path;
    }

    /**
     * Returns the pathname associated with this instance.
     * 
     * @return The associated pathname.
     */
    File getPath() {
        return path;
    }

    /**
     * Returns the absolute abstract pathname of this instance's file resolved
     * against a given directory.
     * 
     * @param dirPath
     *            The directory against which to resolve the pathname.
     */
    File getFile(final File dirPath) {
        return new File(dirPath, path.getPath());
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
