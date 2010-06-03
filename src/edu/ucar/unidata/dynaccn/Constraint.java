/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;

/**
 * A constraint on a string attribute.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Constraint implements Comparable<Constraint>, Serializable {
    /**
     * Relational operators for defining constraints.
     * 
     * @author Steven R. Emmerson
     */
    private static enum Operator {
        EQUAL_TO {
            @Override
            boolean execute(final Attribute attribute, final Object value1,
                    final Object value2) {
                return attribute.areEqual(value1, value2);
            }

            @Override
            boolean exactlySpecifies(final Attribute attribute,
                    final Object value1, final Object value2) {
                return attribute.areEqual(value1, value2);
            }
        },
        NOT_EQUAL_TO {
            @Override
            boolean execute(final Attribute attribute, final Object value1,
                    final Object value2) {
                return attribute.areNotEqual(value1, value2);
            }
        };

        /**
         * Indicates if two attribute values satisfy the constraint.
         * 
         * @param value1
         *            The first attribute value.
         * @param value2
         *            The second attribute value or {@code null}. If not {@code
         *            null}, then it must have the same type as {@code value1}.
         * @return {@code true} if and only if the two attribute values satisfy
         *         this constraint.
         * @throws IllegalArgumentException
         *             if {@code value2 != null && !value1.isSameType(value2)}.
         */
        abstract boolean execute(Attribute attribute, Object value1,
                Object value2);

        /**
         * Indicates if one attribute value exactly specifies another according
         * to this instance.
         * 
         * This implementation returns {@code false}.
         * 
         * @param value1
         *            The first attribute value.
         * @param value2
         *            The second attribute value.
         * @return {@code true} if and only if this instance and the first value
         *         exactly specify the second.
         */
        boolean exactlySpecifies(final Attribute attribute,
                final Object value1, final Object value2) {
            return false; // default
        }
    }

    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The attribute.
     */
    private final Attribute   attribute;
    /**
     * The constraining value.
     */
    private final Object      value;
    /**
     * The constraining relational operator.
     */
    private final Operator    operator;

    /**
     * Constructs from an attribute-value.
     * 
     * @param attribute
     *            The attribute.
     * @param value
     *            The attribute's value.
     * @param operator
     *            The relational operator.
     * @throws if {@code attribute == null || value == null || operator == null}
     *         .
     */
    private Constraint(final Attribute attribute, final Object value,
            final Operator operator) {
        if (null == attribute || null == value || null == operator) {
            throw new NullPointerException();
        }

        this.attribute = attribute;
        this.value = value;
        this.operator = operator;
    }

    /**
     * Returns the associated attribute.
     * 
     * @return The associated attribute.
     */
    Attribute getAttribute() {
        return attribute;
    }

    /**
     * Returns the associated attribute's value.
     * 
     * @return The associated attribute's value.
     */
    Object getAttributeValue() {
        return value;
    }

    /**
     * Returns an equality constraint on an attribute's value.
     * 
     * @param attribute
     *            The attribute.
     * @param value
     *            The constraining value.
     * @return An equality constraint for the given value.
     */
    static Constraint equalTo(final Attribute attribute, final Object value) {
        return new Constraint(attribute, value, Operator.EQUAL_TO);
    }

    /**
     * Returns an inequality constraint on an attribute's value.
     * 
     * @param attribute
     *            The attribute to constrain.
     * @param value
     *            The constraining value.
     * @return An inequality constraint for the given value.
     */
    static Constraint notEqualTo(final Attribute attribute, final Object value) {
        return new Constraint(attribute, value, Operator.NOT_EQUAL_TO);
    }

    /**
     * Indicates if a value satisfies this constraint.
     * 
     * @value The value or {@code null}.
     * @return {@code true} if and only if the given value satisfies this
     *         constraint.
     * @throws NullPointerException
     *             if {@code value == null}.
     */
    boolean satisfiedBy(final Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        return operator.execute(attribute, this.value, value);
    }

    /**
     * Indicates if this instance exactly specifies a particular value.
     * 
     * @param value
     *            The attribute's value or {@code null}.
     * @return {@code true} if and only if {@code value != null} and this
     *         instance is satisfied by the given value and no other.
     */
    boolean exactlySpecifies(final Object value)
            throws IllegalArgumentException {
        return operator.exactlySpecifies(this.attribute, this.value, value);
    }

    /**
     * Compares this instance to another instance. Only the types of constraints
     * (e.g., equality) and the associated attributes are considered: the
     * attribute values are ignored.
     * 
     * @param that
     *            The other instance.
     * @return A value less than, equal to, or greater than zero as this
     *         instance is considered less than, equal to, or greater than the
     *         other instance, respectively.
     */
    public int compareTo(final Constraint that) {
        int cmp = attribute.compareTo(that.attribute);

        if (0 == cmp) {
            cmp = operator.compareTo(that.operator);
        }

        return cmp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        return obj instanceof Constraint && compareTo((Constraint) obj) == 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((operator == null)
                ? 0
                : operator.hashCode());
        result = prime * result + ((value == null)
                ? 0
                : value.hashCode());
        return result;
    }
}
