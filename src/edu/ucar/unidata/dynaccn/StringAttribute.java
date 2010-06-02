/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import net.jcip.annotations.Immutable;

/**
 * A file-attribute of string type.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
@Immutable
final class StringAttribute extends Attribute {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs from a name.
     * 
     * @param name
     *            The name of the attribute.
     */
    StringAttribute(final String name) {
        super(name, String.class);
    }

    @Override
    AttributeEntry getAttributeValue(final String string) {
        return new AttributeEntry(this, string);
    }

    @Override
    boolean areEqual(final Object value1, final Object value2) {
        if (!(value1 instanceof String && (null == value2 || value2 instanceof String))) {
            throw new IllegalArgumentException();
        }
        return ((String) value1).equals(value2);
    }

    @Override
    boolean areNotEqual(final Object value1, final Object value2) {
        if (!(value1 instanceof String && (null == value2 || value2 instanceof String))) {
            throw new IllegalArgumentException();
        }
        return !value1.equals(value2);
    }

    private Object readResolve() {
        return new StringAttribute(name);
    }
}
