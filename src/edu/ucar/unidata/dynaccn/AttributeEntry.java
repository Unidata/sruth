/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A value for an attribute.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class AttributeEntry {
    /**
     * The corresponding attribute.
     */
    private final Attribute attribute;
    /**
     * The attribute's value.
     */
    private final Object    value;

    /**
     * Constructs from an attribute and a value.
     * 
     * @param attribute
     *            The attribute.
     * @param value
     *            The value.
     * @throws NullPointerException
     *             if {@code attribute == null || value == null}.
     */
    AttributeEntry(final Attribute attribute, final Object value) {
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
     * Returns the value.
     * 
     * @return The value.
     */
    Object getValue() {
        return value;
    }

    /**
     * Returns the string form of the value.
     * 
     * @return The string form of the value.
     */
    public String getValueString() {
        return value.toString();
    }

    /**
     * Indicates whether or not another instance has the same type as this
     * instance (i.e., same name and value-type}.
     * 
     * @param value
     *            The other attribute value.
     * @return {@code true} if and only if this type equals that type.
     */
    boolean isSameType(final AttributeEntry value2) {
        return attribute.equals(value2.getAttribute());
    }

    /**
     * Returns an equality constraint on this instance.
     * 
     * @return An equality constraint on this instance.
     */
    Constraint equalTo() {
        return Constraint.equalTo(attribute, value);
    }

    /**
     * Indicates if the value of this instance is equal to an object.
     * 
     * @param value
     *            The object
     * @return {@code true} if and only if the value of this instance equals the
     *         object.
     */
    final boolean equalTo(final Object value) {
        return attribute.areEqual(this.value, value);
    }

    /**
     * Indicates if the value of this instance is not equal to an object.
     * 
     * @param value
     *            The object
     * @return {@code true} if and only if the value of this instance doesn't
     *         equal the object.
     */
    final boolean notEqualTo(final Object value) {
        return attribute.areNotEqual(this.value, value);
    }

    /**
     * Returns an inequality constraint based on this instance.
     * 
     * @return An inequality constraint based on this instance.
     */
    Constraint notEqualTo() {
        return Constraint.notEqualTo(attribute, value);
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
        result = prime * result + ((attribute == null)
                ? 0
                : attribute.hashCode());
        result = prime * result + ((value == null)
                ? 0
                : value.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AttributeEntry other = (AttributeEntry) obj;
        if (attribute == null) {
            if (other.attribute != null) {
                return false;
            }
        }
        else if (!attribute.equals(other.attribute)) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        }
        else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "AttributeEntry [attribute=" + attribute + ", value=" + value
                + "]";
    }
}
