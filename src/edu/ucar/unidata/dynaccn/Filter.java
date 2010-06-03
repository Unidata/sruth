/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

/**
 * A conjunction of constraints on the attributes of a file.
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
    static final Filter                 EVERYTHING       = new Filter("glob:**") {
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
    static final Filter                 NOTHING          = new Filter("glob:/") {
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
     * The original pattern-syntax and pattern.
     * 
     * @serial The pattern-syntax and pattern.
     */
    private final String                syntaxAndPattern;
    /**
     * The original pattern as a pathname.
     */
    private volatile transient Path     patternPath;
    /**
     * The path matcher.
     */
    private final transient PathMatcher matcher;

    /**
     * Constructs from a pattern.
     * 
     * @param syntaxAndPattern
     *            The pattern-syntax and pattern as specified by
     *            {@link FileSystem#getPathMatcher(String)}.
     * @throws IllegalArgumentException
     *             if {@code syntaxAndPattern} is invalid.
     * @throws NullPointerException
     *             if {@code syntaxAndPattern == null}.
     */
    Filter(final String syntaxAndPattern) {
        final int index = syntaxAndPattern.indexOf(':');
        if (-1 == index) {
            throw new IllegalArgumentException(syntaxAndPattern);
        }
        this.syntaxAndPattern = syntaxAndPattern;
        try {
            patternPath = Paths.get(syntaxAndPattern.substring(index + 1));
        }
        catch (final InvalidPathException ignored) {
        }
        matcher = FileSystems.getDefault().getPathMatcher(syntaxAndPattern);
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
        return new Filter(syntaxAndPattern);
    }
}
