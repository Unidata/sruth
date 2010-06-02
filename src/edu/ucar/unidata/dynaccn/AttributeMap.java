/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.jcip.annotations.ThreadSafe;

/**
 * A map from attributes to their values.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class AttributeMap implements Iterable<AttributeEntry> {
    /**
     * The map from attributes to their values.
     */
    private final ConcurrentMap<Attribute, Object> map = new ConcurrentHashMap<Attribute, Object>();

    /**
     * Adds an attribute-entry.
     * 
     * @param entry
     *            The attribute-entry.
     * @throws IllegalArgumentException
     *             if {@code entry#getAttribute()} is already in the map.
     */
    void add(final AttributeEntry entry) {
        if (map.putIfAbsent(entry.getAttribute(), entry.getValue()) != null) {
            throw new IllegalArgumentException("Already present: "
                    + entry.toString());
        }
    }

    /**
     * Maps an attribute to a value.
     * 
     * @param attribute
     *            The attribute.
     * @param value
     *            The value
     * @throws IllegalArgumentException
     *             if {@code attribute} is already in the map.
     */
    void put(final Attribute attribute, final Object value) {
        if (map.putIfAbsent(attribute, value) != null) {
            throw new IllegalArgumentException("Already present: " + attribute);
        }
    }

    /**
     * Returns the value of a given attribute.
     * 
     * @param attribute
     *            The attribute.
     * @return The value of the attribute in this instance or {@code null} if
     *         this instance doesn't have the attribute.
     */
    Object get(final Attribute attribute) {
        return map.get(attribute);
    }

    /**
     * Returns the number of attributes.
     * 
     * @return The number of attributes.
     */
    int size() {
        return map.size();
    }

    /**
     * Indicates if this instance contains all the attribute-entries of another
     * instance.
     * 
     * @param that
     *            The other instance.
     * @return {@code true} if and only if this instance contains all the
     *         attribute-entries of the other instance.
     */
    boolean containsAll(final AttributeMap that) {
        return map.entrySet().containsAll(that.map.entrySet());
    }

    @Override
    public Iterator<AttributeEntry> iterator() {
        return new SimpleIterator<AttributeEntry>() {
            private Iterator<Map.Entry<Attribute, Object>> iter;

            @Override
            protected AttributeEntry getNext() {
                if (null == iter) {
                    iter = map.entrySet().iterator();
                }
                if (iter.hasNext()) {
                    final Map.Entry<Attribute, Object> entry = iter.next();
                    return new AttributeEntry(entry.getKey(), entry.getValue());
                }
                return null;
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "AttributeMap [map=" + map + "]";
    }
}
