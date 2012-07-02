/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

/**
 * A consumer of file-based data specifications.
 * 
 * @author Steven R. Emmerson
 */
interface FilePieceSpecSetConsumer {
    /**
     * Consumes a specification of data.
     * 
     * @param spec
     *            The data specification
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    void consume(final FilePieceSpecSet spec) throws InterruptedException;
}
