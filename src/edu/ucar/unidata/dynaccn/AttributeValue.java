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
final class AttributeValue {
    /**
     * The value.
     */
    private final String value;

    /**
     * Constructs from a value.
     * 
     * @param value
     *            The value.
     * @throws NullPointerException
     *             if {@code value == null}.
     */
    AttributeValue(final String value) {
        if (null == value) {
            throw new NullPointerException();
        }

        this.value = value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AttributeValue other = (AttributeValue) obj;
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
}
