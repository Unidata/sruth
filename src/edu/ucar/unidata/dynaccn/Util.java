/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * Utility class containing useful static methods.
 * 
 * @author Steven R. Emmerson
 */
final class Util {
    /**
     * Launder a {@link Throwable}. If the throwable is an error, then throw it;
     * if it's a {@link RuntimeException}, then return it; otherwise, throw the
     * runtime-exception {@link IllegalStateException} to indicate a logic
     * error.
     */
    static RuntimeException launderThrowable(final Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        else if (t instanceof Error) {
            throw (Error) t;
        }
        else {
            throw new IllegalStateException("Unhandled checked exception", t);
        }
    }
}
