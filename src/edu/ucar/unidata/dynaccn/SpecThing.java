package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * A specification of data that's exchanged on the NOTICE and REQUEST lines.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
abstract class SpecThing implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Process this instance using the local peer.
     * 
     * @param requestReceiver
     *            The local peer.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    abstract void process(SpecProcessor processor) throws IOException,
            InterruptedException;

    /**
     * Returns a new instance.
     * 
     * @param specs
     *            Specifications of available data.
     * @return A new instance containing the given data specifications.
     */
    static final SpecThing newInstance(final Collection<PiecesSpec> specs) {
        return (specs.size() == 1)
                ? new FileThing(specs.iterator().next())
                : new FileSetThing(specs);
    }

    @Override
    public abstract String toString();
}