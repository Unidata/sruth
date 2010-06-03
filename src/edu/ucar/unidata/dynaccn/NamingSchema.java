/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * The naming scheme for converting between sets of attribute-values and
 * pathnames.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class NamingSchema {
    /**
     * The singleton instance.
     */
    @GuardedBy("NamingSchema.class")
    private static NamingSchema           INSTANCE;
    /**
     * The mapping from attribute to pathname name-index.
     */
    @GuardedBy("this")
    private final Map<Attribute, Integer> indexes = new HashMap<Attribute, Integer>();
    /**
     * The mapping from pathname name-index to attribute.
     */
    @GuardedBy("this")
    private final Attribute[]             attributes;

    /**
     * Constructs from an of attributes. The order of attributes in the array
     * specifies the order of pathname names: the first attribute specifies the
     * first pathname name, etc..
     * 
     * @param attributes
     *            The array of attributes.
     * @throws NullPointerException
     *             if {@code attributes == null}.
     */
    private NamingSchema(final Attribute[] attributes) {
        int index = 0;
        for (final Attribute attribute : attributes) {
            indexes.put(attribute, index++);
        }
        this.attributes = attributes.clone();
    }

    /**
     * Returns an instance.
     * 
     * @throws IllegalStateException
     *             if {@link #initialize(List<Attribute>)} hasn't been called.
     */
    static synchronized NamingSchema getInstance() {
        if (null == INSTANCE) {
            throw new IllegalStateException();
        }
        return INSTANCE;
    }

    /**
     * Initializes this class from an array of attributes. The order of
     * attributes in the array specifies the order of pathname names: the first
     * attribute specifies the first pathname name, etc..
     * 
     * @param attributes
     *            The array of attributes.
     * @throws IllegalStateException
     *             if this method has already been called.
     */
    static synchronized void initialize(final Attribute[] attributes) {
        if (null != INSTANCE) {
            throw new IllegalStateException();
        }
        INSTANCE = new NamingSchema(attributes);
    }

    /**
     * Returns the pathname form of a collection of attribute-values.
     * 
     * @param values
     *            The collection of attribute-values.
     * @return The corresponding pathname form.
     * @throws IllegalArgumentException
     *             if there is no corresponding pathname form.
     */
    synchronized Path getPath(final Collection<AttributeEntry> values) {
        final List<String> names = new ArrayList<String>(values.size());
        for (final AttributeEntry value : values) {
            final Integer index = indexes.get(value.getAttribute());
            if (null == index) {
                throw new IllegalArgumentException();
            }
            names.add(index, value.getValueString());
        }
        final StringBuilder buf = new StringBuilder();
        for (final String name : names) {
            if (null == name) {
                throw new IllegalArgumentException();
            }
            buf.append(name);
            buf.append(File.pathSeparator);
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length() - 1);
        }
        return Paths.get(buf.toString());
    }

    /**
     * Returns the collection of attribute-values form of a pathname.
     * 
     * @param path
     *            The pathname.
     * @return The corresponding collection of attribute-values.
     * @throws IllegalArgumentException
     *             if the pathname is absolute.
     */
    synchronized Collection<AttributeEntry> getAttributes(final Path path) {
        if (path.isAbsolute()) {
            throw new IllegalArgumentException();
        }
        final int nameCount = path.getNameCount();
        final List<AttributeEntry> entries = new ArrayList<AttributeEntry>(
                nameCount);
        for (int i = 0; i < nameCount; i++) {
            final Path name = path.getName(i);
            final AttributeEntry value = this.attributes[i]
                    .getAttributeEntry(name.toString());
            entries.add(value);
        }
        return entries;
    }

    /**
     * Returns the attribute associated with a given index.
     * 
     * @param i
     *            The index of the attribute.
     * @return The associated attribute.
     * @throws IndexOutOfBoundsException
     *             if {@code i} is invalid.
     */
    Attribute getAttribute(final int i) {
        return attributes[i];
    }
}
