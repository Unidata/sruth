/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An empty set of piece-specifications.
 * 
 * The singleton instance is immutable.
 * 
 * @author Steven R. Emmerson
 */
final class EmptyPieceSpecSet implements PieceSpecSetIface {
    /**
     * The serial version identifier.
     */
    private static final long             serialVersionUID = 1L;
    /**
     * The singleton instance.
     */
    public static final EmptyPieceSpecSet INSTANCE         = new EmptyPieceSpecSet();

    /**
     * Constructs from nothing.
     */
    private EmptyPieceSpecSet() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<PieceSpec> iterator() {
        return new Iterator<PieceSpec>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public PieceSpec next() {
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.PieceSpecSetIface#merge(edu.ucar.unidata.sruth
     * .PieceSpecSetIface )
     */
    @Override
    public PieceSpecSetIface merge(final PieceSpecSetIface specs) {
        return specs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.PieceSpecSetIface#merge(edu.ucar.unidata.sruth.
     * PieceSpecSet)
     */
    @Override
    public PieceSpecSetIface merge(final PieceSpecSet specs) {
        return specs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.PieceSpecSetIface#merge(edu.ucar.unidata.sruth.
     * FilePieceSpecs)
     */
    @Override
    public PieceSpecSetIface merge(final FilePieceSpecs specs) {
        return specs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.PieceSpecSetIface#merge(edu.ucar.unidata.sruth
     * .PieceSpec )
     */
    @Override
    public PieceSpecSetIface merge(final PieceSpec spec) {
        return spec;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.PieceSpecSetIface#remove(edu.ucar.unidata.sruth
     * .PieceSpec )
     */
    @Override
    public PieceSpecSetIface remove(final PieceSpec spec) {
        return INSTANCE;
    }

    @Override
    public boolean contains(final PieceSpec spec) {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.PieceSpecSetIface#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public EmptyPieceSpecSet clone() {
        return this; // because this instance is an immutable singleton
    }
}
