/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A consumer of file-based data specifications.
 * 
 * @author Steven R. Emmerson
 */
interface PiecesSpecConsumer {
    void consume(final FilePieceSpecSet spec);
}
