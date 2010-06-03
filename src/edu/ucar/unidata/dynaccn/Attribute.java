/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;

/**
 * A file attribute comprising a name and a string type.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
abstract class Attribute implements Comparable<Attribute>, Serializable {
    /**
     * The serial version ID.
     */
    private static final long     serialVersionUID = 1L;
    /**
     * The name of the attribute.
     */
    protected final String        name;
    /**
     * The type of the attribute.
     */
    @SuppressWarnings("unchecked")
    private final transient Class type;

    /**
     * Constructs from the name of the attribute and the type.
     * 
     * @param name
     *            The name of the attribute.
     * @param type
     *            The type of the attribute.
     * @throws NullPointerException
     *             if {@code name == null || type == null}.
     */
    @SuppressWarnings("unchecked")
    Attribute(final String name, final Class type) {
        if (null == name || null == type) {
            throw new NullPointerException();
        }

        this.name = name;
        this.type = type;
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
     * Returns the type of this instance.
     * 
     * @return {@code String.class}.
     */
    @SuppressWarnings("unchecked")
    Class getType() {
        return type;
    }

    /**
     * Indicates if two values for this attribute are equal.
     * 
     * @param value1
     *            The first value.
     * @param value2
     *            The second value or {@code null}.
     * @return {@code true} if and only if the values are equal.
     * @throws IllegalArgumentException
     *             if one or both of the arguments have the wrong type.
     */
    abstract boolean areEqual(Object value1, Object value2);

    /**
     * Indicates if two values for this attribute are not equal.
     * 
     * @param value1
     *            The first value.
     * @param value2
     *            The second value or {@code null}.
     * @return {@code true} if and only if the values are not equal.
     * @throws IllegalArgumentException
     *             if one or both of the arguments have the wrong type.
     */
    abstract boolean areNotEqual(Object value1, Object value2);

    /**
     * Returns the attribute-value corresponding to this instance and the string
     * form of the value.
     * 
     * @param string
     *            The string form of the value.
     * @return The corresponding attribute-value.
     */
    abstract AttributeEntry getAttributeEntry(String string);

    /**
     * Compares this instance to another.
     * 
     * @param that
     *            The other instance.
     * @return A value less than, equal to, or greater than zero as this
     *         instance is considered less than, equal to, or greater than the
     *         other instance, respectively.
     */
    public final int compareTo(final Attribute that) {
        int cmp = name.compareTo(that.getName());
        if (0 == cmp) {
            final int i1 = System.identityHashCode(type);
            final int i2 = System.identityHashCode(that.type);
            cmp = i1 < i2
                    ? -1
                    : i1 == i2
                            ? 0
                            : 1;
        }
        return cmp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null)
                ? 0
                : name.hashCode());
        result = prime * result + ((type == null)
                ? 0
                : type.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Attribute other = (Attribute) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        }
        else if (!name.equals(other.name)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        }
        else if (!type.equals(other.type)) {
            return false;
        }
        return true;
    }
}
