/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decodes an encoding of an object, returning the corresponding object.
 * 
 * @author Steven R. Emmerson
 */
interface Decoder<T> {
    /**
     * Decodes an object from an input stream.
     * 
     * @param input
     *            The input stream.
     * @return an object of type {@code T}.
     * @throws IOException
     */
    T decode(InputStream input) throws IOException;
}
