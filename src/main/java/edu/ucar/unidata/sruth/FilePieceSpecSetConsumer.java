/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
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
    void consume(final FilePieceSpecSet spec);
}
