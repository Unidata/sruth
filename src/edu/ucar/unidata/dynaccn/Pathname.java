/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for pathnames.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Pathname {
    /**
     * The name of the hidden directory that will be ignored for the most part.
     */
    private static final Path HIDDEN_DIR = Paths.get(".dynaccn");

    /**
     * Indicates whether or not a directory is hidden.
     * 
     * @param dir
     *            Pathname of the directory in question.
     * @return {@code true} if and only if the directory is hidden.
     * @throws NullPointerException
     *             if {@code dir == null}.
     */
    static boolean isHidden(final Path dir) {
        return dir.endsWith(HIDDEN_DIR);
    }

    /**
     * Returns the hidden form of a visible pathname.
     * 
     * @param path
     *            Pathname of the file to be hidden.
     * @return The hidden pathname.
     */
    static Path hide(final Path path) {
        final Path parent = path.getParent();
        final Path hiddenDir;
        if (null == parent) {
            hiddenDir = HIDDEN_DIR;
        }
        else {
            if (parent.endsWith(HIDDEN_DIR)) {
                throw new IllegalArgumentException("Hidden: " + path);
            }
            hiddenDir = parent.resolve(HIDDEN_DIR);
        }
        return hiddenDir.resolve(path.getName());
    }

    /**
     * Returns the visible form of a hidden pathname.
     * 
     * @param path
     *            The hidden pathname.
     * @return The visible pathname.
     * @throws IllegalArgumentException
     *             if {@code path} isn't hidden.
     */
    static Path reveal(final Path path) {
        final int count = path.getNameCount();
        if (!path.subpath(count - 2, count - 1).equals(HIDDEN_DIR)) {
            throw new IllegalArgumentException("Not hidden: " + path);
        }
        return path.getRoot().resolve(path.subpath(0, count - 2)).resolve(
                path.getName());
    }
}
