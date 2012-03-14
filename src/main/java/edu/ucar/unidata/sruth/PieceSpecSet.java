/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import edu.ucar.unidata.sruth.Connection.Message;

/**
 * The interface to one or more piece-specifications.
 * 
 * @author Steven R. Emmerson
 */
interface PieceSpecSet extends Iterable<PieceSpec>, Message {
    /**
     * Merges this instance with one or more piece-specifications. The returned
     * instance might be this instance, or the other instance, or a new
     * instance. Instances that aren't returned remain unmodified.
     * 
     * @param specs
     *            One or more piece-specifications to be merged with this one.
     * @return The merger of the two instances.
     * @throws NullPointerException
     *             if {@code specs == null}.
     */
    abstract PieceSpecSet merge(PieceSpecSet specs);

    /**
     * Merges this instance with piece-specifications for one or more files. The
     * returned instance might be this instance, or the other instance, or a new
     * instance. Instances that aren't returned remain unmodified.
     * 
     * @param specs
     *            Piece-specification for one or more files.
     * @return The merger of the two instances.
     * @throws NullPointerException
     *             if {@code specs == null}.
     */
    abstract PieceSpecSet merge(MultiFilePieceSpecs specs);

    /**
     * Merges this instance with multiple piece-specification for a file. The
     * returned instance might be this instance, or the other instance, or a new
     * instance. Instances that aren't returned remain unmodified.
     * 
     * @param specs
     *            The piece-specifications for a file.
     * @return The merger of the two instances.
     * @throws IllegalArgumentException
     *             if the file-identifier of the given specifications equals
     *             that of this instance but the file-informations differ.
     * @throws NullPointerException
     *             if {@code specs == null}.
     */
    abstract PieceSpecSet merge(FilePieceSpecs specs);

    /**
     * Merges this instance with a piece-specification. The returned instance
     * might be this instance, or the other instance, or a new instance.
     * Instances that aren't returned remain unmodified.
     * 
     * @param pieceSpec
     *            Specification of the piece of data.
     * @return The merger of the two instances.
     * @throws IllegalArgumentException
     *             if the file-identifier of the given specification equals that
     *             of this instance but the file-informations differ.
     * @throws NullPointerException
     *             if {@code pieceSpec == null}.
     */
    abstract PieceSpecSet merge(PieceSpec pieceSpec);

    abstract boolean isEmpty();
}
