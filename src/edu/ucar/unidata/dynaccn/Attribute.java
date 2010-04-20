/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A file attribute comprising a name and a string type.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Attribute implements Comparable<Attribute> {
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
    Constraint equalTo(final String value) {
        return Constraint.equalTo(this, value);
    }

    /**
     * Returns an inequality constraint on this instance.
     * 
     * @param value
     *            The constraining value.
     */
    Constraint notEqualTo(final String value) {
        return Constraint.notEqualTo(this, value);
    }

    /**
     * Returns the type of this instance.
     * 
     * @return {@code String.class}.
     */
    Class<String> getType() {
        return String.class;
    }

    /**
     * Compares this instance to another.
     * 
     * @param that
     *            The other instance.
     * @return A value less than, equal to, or greater than zero as this
     *         instance is considered less than, equal to, or greater than the
     *         other instance, respectively.
     */
    public int compareTo(final Attribute that) {
        return name.compareTo(that.getName());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null)
                ? 0
                : name.hashCode());
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
        final Attribute other = (Attribute) obj;
        return 0 == compareTo(other);
    }
}
