/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A set of file or category specifications in which no specification is a
 * subset of another.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class FileSetSpec implements Serializable, Iterable<FileId> {
    /**
     * The serial version ID.
     */
    private static final long                                serialVersionUID = 1L;
    /**
     * The set of specifications. No specification in the set is a subset of
     * another.
     */
    @GuardedBy("this")
    private final Set<FileId>                                specs            = new TreeSet<FileId>();
    /**
     * The actual map from individual attribute values to specifications.
     */
    @GuardedBy("this")
    private final transient Map<AttributeEntry, Set<FileId>> inverseMap       = new HashMap<AttributeEntry, Set<FileId>>();

    /**
     * Indicates if this instance is empty or not.
     * 
     * @return {@code true} if and only if this instance is empty.
     */
    synchronized boolean isEmpty() {
        return specs.size() == 0;
    }

    /**
     * Returns the number of file or category specifications in this instance.
     * 
     * @return The number of file or category specifications.
     */
    synchronized int size() {
        return specs.size();
    }

    /**
     * Removes all elements of this instance.
     */
    synchronized void clear() {
        specs.clear();
        inverseMap.clear();
    }

    /**
     * Adds a specification of a file or category.
     * 
     * @param spec
     *            The specification of a file or category.
     */
    public synchronized void add(final FileId spec) {
        if (!specs.contains(spec)) {
            if (!alreadyHaveProperSubsetOf(spec)) {
                removeAllSupersetsOf(spec);
                specs.add(spec);
                addToInverseMap(spec);
            }
        }
    }

    /**
     * Indicates if this instance already has a specification that's a proper
     * subset of a given specification (and, hence, more general).
     * 
     * @param target
     *            The given specification.
     * @return {@code true} if and only if this instance already has a
     *         specification that's a proper subset of the given specification.
     */
    @GuardedBy("this")
    private boolean alreadyHaveProperSubsetOf(final FileId target) {
        for (final FileId spec : specs) {
            if (spec.size() < target.size()) {
                if (target.containsAll(spec)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Adds a specification to the inverse map.
     * 
     * @param spec
     *            The specification to add.
     */
    @GuardedBy("this")
    private void addToInverseMap(final FileId spec) {
        for (final AttributeEntry value : spec) {
            Set<FileId> specs = inverseMap.get(value);
            if (null == specs) {
                specs = new TreeSet<FileId>();
                inverseMap.put(value, specs);
            }
            specs.add(spec);
        }
    }

    /**
     * Removes all specifications that are supersets of (and, hence, more
     * restrictive than) a given specification.
     * 
     * @param spec
     *            The given specification.
     */
    @GuardedBy("this")
    private void removeAllSupersetsOf(final FileId target) {
        Set<FileId> specSet = null;
        for (final AttributeEntry value : target) {
            final Set<FileId> specs = inverseMap.get(value);
            if (null == specs) {
                return;
            }
            if (null == specSet) {
                specSet = specs;
            }
            else {
                specSet.retainAll(specs);
                if (specSet.isEmpty()) {
                    return;
                }
            }
        }
        if (null == specSet) {
            return;
        }
        for (final FileId spec : specSet) {
            for (final AttributeEntry entry : spec) {
                inverseMap.get(entry.getAttribute()).remove(spec);
            }
        }
        specs.removeAll(specSet);
    }

    @Override
    public Iterator<FileId> iterator() {
        return specs.iterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FileSetSpec [specs=" + specs + "]";
    }

    private Object readResolve() throws InvalidObjectException {
        final FileSetSpec set = new FileSetSpec();
        for (final FileId spec : specs) {
            set.add(spec);
        }
        return set;
    }
}
