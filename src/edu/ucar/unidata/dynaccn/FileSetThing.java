package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * A specification of pieces of data in multiple files.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileSetThing extends SpecThing {
    /**
     * The serial version ID.
     */
    private static final long      serialVersionUID = 1L;
    /**
     * The list of file-dependent data specifications.
     */
    protected final List<PiecesSpec> specs            = new LinkedList<PiecesSpec>();

    /**
     * Constructs from data-specifications.
     * 
     * @param specs
     *            The data-specifications.
     * @throws NullPointerException
     *             if {@code specs == null}.
     */
    FileSetThing(final Collection<PiecesSpec> specs) {
        if (null == specs) {
            throw new NullPointerException();
        }
        this.specs.addAll(specs);
    }

    @Override
    void process(final SpecProcessor processor) throws IOException,
            InterruptedException {
        for (final PiecesSpec spec : specs) {
            spec.processYourself(processor);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{specs=(" + specs.size()
                + " elements)";
    }

    private Object readResolve() {
        return new FileSetThing(specs);
    }
}