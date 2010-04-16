/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A file attribute comprising a name.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Attribute {
    /**
     * The name of the attribute.
     */
    private final String name;

    /**
     * Constructs from the name of the attribute.
     * 
     * @param name
     *            The name of the attribute.
     * @throws NullPointerException
     *             if {@code name == null}.
     */
    Attribute(final String name) {
        if (null == name) {
            throw new NullPointerException();
        }

        this.name = name;
    }

    /**
     * Returns the name of the attribute.
     * 
     * @return The name of the attribute.
     */
    String getName() {
        return name;
    }

    /**
     * Returns an equality constraint on this instance.
     * 
     * @param value
     *            The constraining value.
     */
    Constraint equalityConstraint(final AttributeValue value) {
        return new Constraint(this, value);
    }
}
