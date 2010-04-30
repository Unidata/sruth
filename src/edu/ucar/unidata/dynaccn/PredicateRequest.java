package edu.ucar.unidata.dynaccn;

import java.io.IOException;

/**
 * A request that specifies the desired data.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class PredicateRequest extends Request {
    /**
     * Serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The specification of desired data.
     */
    private final Predicate   predicate;

    /**
     * Constructs from a specification of desired data.
     * 
     * @param predicate
     *            Specification of desired data.
     */
    PredicateRequest(final Predicate predicate) {
        if (null == predicate) {
            throw new NullPointerException();
        }
        this.predicate = predicate;
    }

    /**
     * Returns the specification of desired data.
     * 
     * @return The specification of desired data.
     */
    Predicate getPredicate() {
        return predicate;
    }

    @Override
    void process(final Peer peer) throws InterruptedException, IOException {
        peer.process(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{predicate=" + predicate + "}";
    }
}