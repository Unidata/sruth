/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

/**
 * A filter for selecting a class of files.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
class Filter implements Serializable {
    /**
     * The serial version ID.
     */
    private static final long           serialVersionUID = 1L;
    /**
     * The filter that is satisfied by everything.
     */
    static final Filter                 EVERYTHING       = new Filter("**") {
                                                             /**
                                                              * The serial
                                                              * version ID.
                                                              */
                                                             private static final long serialVersionUID = 1L;

                                                             @Override
                                                             public String toString() {
                                                                 return "EVERYTHING";
                                                             }

                                                             private Object readResolve() {
                                                                 return EVERYTHING;
                                                             }
                                                         };
    /**
     * The filter that is satisfied by nothing.
     */
    static final Filter                 NOTHING          = new Filter("/") {
                                                             /**
                                                              * The serial
                                                              * version ID.
                                                              */
                                                             private static final long serialVersionUID = 1L;

                                                             @Override
                                                             boolean satisfiedBy(
                                                                     final FileInfo fileInfo) {
                                                                 return false;
                                                             }

                                                             @Override
                                                             boolean exactlySpecifies(
                                                                     final FileInfo fileInfo) {
                                                                 return false;
                                                             }

                                                             @Override
                                                             public String toString() {
                                                                 return "NOTHING";
                                                             }

                                                             private Object readResolve() {
                                                                 return NOTHING;
                                                             }
                                                         };
    /**
     * The original pattern.
     * 
     * @serial The pattern.
     */
    private final String                pattern;
    /**
     * The original pattern as a pathname or {@code null} if the pattern isn't a
     * valid pathname.
     */
    private volatile transient Path     patternPath;
    /**
     * The path matcher.
     */
    private final transient PathMatcher matcher;

    /**
     * Constructs from a pattern.
     * 
     * @param pattern
     *            The glob pattern as described by
     *            {@link java.nio.file.FileSystem#getPathMatcher(String)}.
     * @throws IllegalArgumentException
     *             if {@code glob} is invalid.
     * @throws NullPointerException
     *             if {@code glob == null}.
     */
    Filter(final String pattern) {
        if (null == pattern) {
            throw new NullPointerException();
        }
        this.pattern = pattern;
        try {
            patternPath = Paths.get(pattern);
        }
        catch (final InvalidPathException ignored) {
        }
        matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }

    /**
     * Indicates if a file satisfies this filter.
     * 
     * @param fileInfo
     *            A description of the file.
     * @return {@code true} if and only if the file satisfies this filter.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    boolean satisfiedBy(final FileInfo fileInfo) {
        return matcher.matches(fileInfo.getPath());
    }

    /**
     * Indicates if this instance is satisfied by, and only by, a given file.
     * 
     * @param fileInfo
     *            The file in question.
     * @return {@code true} if and only if this filter is satisfied by, and only
     *         by, the given file.
     */
    boolean exactlySpecifies(final FileInfo fileInfo) {
        return fileInfo.getPath().equals(patternPath);
    }

    private Object readResolve() {
        return new Filter(pattern);
    }
}
