/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * An equality constraint on an attribute comprising an attribute and a value.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Constraint {
    /**
     * The attribute in question.
     */
    private final Attribute      attribute;
    /**
     * The constraining value.
     */
    private final AttributeValue value;

    /**
     * Constructs from an attribute and a value.
     * 
     * @param attribute
     *            The attribute.
     * @param value
     *            The value.
     * @throws if {@code attribute == null || value == null}.
     */
    Constraint(final Attribute attribute, final AttributeValue value) {
        if (null == attribute || null == value) {
            throw new NullPointerException();
        }

        this.attribute = attribute;
        this.value = value;
    }

    /**
     * Indicates if a value satisfies this constraint.
     * 
     * @value The value or {@code null}.
     * @return {@code true} if and only if the given value is equal to the value
     *         of this constraint.
     */
    boolean satisfiedBy(final AttributeValue value) {
        return this.value.equals(value);
    }

    /**
     * Returns the associated attribute.
     * 
     * @return The associated attribute.
     */
    Attribute getAttribute() {
        return attribute;
    }
}
