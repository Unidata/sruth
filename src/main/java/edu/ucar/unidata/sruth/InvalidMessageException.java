/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

/**
 * Thrown to indicate a problem with a high-level protocol message.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class InvalidMessageException extends Exception {
    /**
     * The serial version identifier
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs from a message-string and an underlying cause.
     */
    InvalidMessageException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
