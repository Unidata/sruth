package edu.ucar.unidata.dynaccn;

import java.io.IOException;

/**
 * A specification of pieces of data in a single file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileThing extends SpecThing {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The specification of the data-pieces in the file.
     */
    protected final PiecesSpec  spec;

    /**
     * Constructs from a data-specification.
     * 
     * @param spec
     *            The data-specification.
     * @throws NullPointerException
     *             if {@code spec == null}.
     */
    FileThing(final PiecesSpec spec) {
        if (null == spec) {
            throw new NullPointerException();
        }
        this.spec = spec;
    }

    @Override
    void process(final SpecProcessor processor) throws IOException,
            InterruptedException {
        spec.processYourself(processor);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{spec=" + spec + "}";
    }

    private Object readResolve() {
        return new FileThing(spec);
    }
}