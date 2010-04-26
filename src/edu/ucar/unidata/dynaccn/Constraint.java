/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.io.Serializable;

/**
 * A constraint on a string attribute.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
abstract class Constraint implements Comparable<Constraint>, Serializable {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * An equality constraint on a string attribute.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static final class EqualTo extends Constraint {
        /**
         * The serial version ID.
         */
        private static final long serialVersionUID = 1L;

        /**
         * Constructs from a string attribute and a value.
         * 
         * @throws NullPointerException
         *             if {@code attribute == null || value == null}.
         */
        EqualTo(final Attribute attribute, final String value) {
            super(attribute, value);
        }

        @Override
        boolean protectedSatisfiedBy(final String value) {
            return this.value.equals(value);
        }

        @Override
        boolean exactlySpecifies(final Object value) {
            return this.value.equals(value);
        }

        private Object readResolve() throws InvalidObjectException {
            try {
                return new EqualTo(attribute, value);
            }
            catch (final Exception e) {
                throw (InvalidObjectException) new InvalidObjectException(
                        "Read invalid " + getClass().getSimpleName())
                        .initCause(e);
            }
        }
    }

    /**
     * An inequality constraint on a string attribute.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static final class NotEqualTo extends Constraint {
        /**
         * The serial version ID.
         */
        private static final long serialVersionUID = 1L;

        /**
         * Constructs from a string attribute and a value.
         * 
         * @throws NullPointerException
         *             if {@code attribute == null || value == null}.
         */
        NotEqualTo(final Attribute attribute, final String value) {
            super(attribute, value);
        }

        @Override
        boolean protectedSatisfiedBy(final String value) {
            return !this.value.equals(value);
        }

        @Override
        boolean exactlySpecifies(final Object value) {
            return false;
        }

        private Object readResolve() throws InvalidObjectException {
            try {
                return new NotEqualTo(attribute, value);
            }
            catch (final Exception e) {
                throw (InvalidObjectException) new InvalidObjectException(
                        "Read invalid " + getClass().getSimpleName())
                        .initCause(e);
            }
        }
    }

    /**
     * The string attribute in question.
     */
    protected final Attribute attribute;
    /**
     * The value in question.
     */
    protected final String    value;

    /**
     * Constructs from an attribute and a value.
     * 
     * @param attribute
     *            The attribute.
     * @param value
     *            The value.
     * @throws if {@code attribute == null || value == null}.
     */
    Constraint(final Attribute attribute, final String value) {
        if (null == attribute || null == value) {
            throw new NullPointerException();
        }

        this.attribute = attribute;
        this.value = value;
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
     * Returns an equality constraint on a string attribute.
     * 
     * @param attribute
     *            The attribute to be constrained.
     * @param value
     *            The constraining value.
     * @return A constraint on the attribute that requires equality with the
     *         value.
     */
    static Constraint equalTo(final Attribute attribute, final String value) {
        return new EqualTo(attribute, value);
    }

    /**
     * Returns an inequality constraint on a string attribute.
     * 
     * @param attribute
     *            The attribute to be constrained.
     * @param value
     *            The constraining value.
     * @return A constraint on the attribute that requires inequality with the
     *         value.
     */
    static Constraint notEqualTo(final Attribute attribute, final String value) {
        return new NotEqualTo(attribute, value);
    }

    /**
     * Indicates if a value satisfies this constraint.
     * 
     * @value The value or {@code null}.
     * @return {@code true} if and only if the given value satisfies this
     *         constraint.
     * @throws ClassCastException
     *             if {@code !(value instanceof String)}.
     */
    boolean satisfiedBy(final Object value) {
        return protectedSatisfiedBy((String) value);
    }

    /**
     * Indicates if a string-value satisfies this constraint.
     * 
     * @value The string-value or {@code null}.
     * @return {@code true} if and only if the given string-value satisfies this
     *         constraint.
     */
    abstract boolean protectedSatisfiedBy(final String value);

    /**
     * Indicates if this instance exactly specifies a particular value.
     * 
     * @param value
     *            The value in question.
     * @return {@code true} if and only if this instance is satisfied by the
     *         given value and no other.
     */
    abstract boolean exactlySpecifies(final Object value);

    /**
     * Compares this instance to another instance.
     * 
     * @param that
     *            The other instance.
     * @return A value less than, equal to, or greater than zero as this
     *         instance is considered less than, equal to, or greater than the
     *         other instance, respectively.
     */
    public int compareTo(final Constraint that) {
        int status = attribute.compareTo(that.getAttribute());

        if (0 != status) {
            return status;
        }

        status = System.identityHashCode(getClass())
                - System.identityHashCode(that.getClass());

        if (0 != status) {
            return status;
        }

        return value.compareTo(that.value);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attribute == null)
                ? 0
                : attribute.hashCode());
        result = prime * result + ((value == null)
                ? 0
                : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Constraint)) {
            return false;
        }
        final Constraint other = (Constraint) obj;
        return 0 == compareTo(other);
    }
}
