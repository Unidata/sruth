/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A conjunction of constraints on the attributes of a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
class Filter implements Comparable<Filter>, Serializable {
    /**
     * The serial version ID.
     */
    private static final long     serialVersionUID = 1L;
    /**
     * The filter that is satisfied by everything.
     */
    static final Filter           EVERYTHING       = new Filter(
                                                           new Constraint[0]) {
                                                       /**
                                                        * The serial version ID.
                                                        */
                                                       private static final long serialVersionUID = 1L;

                                                       @Override
                                                       public String toString() {
                                                           return "EVERYTHING";
                                                       }

                                                       private Object readResolve() {
                                                           return EVERYTHING;
                                                       }
                                                   };
    /**
     * The filter that is satisfied by nothing.
     */
    static final Filter           NOTHING          = new Filter(
                                                           new Constraint[0]) {
                                                       /**
                                                        * The serial version ID.
                                                        */
                                                       private static final long serialVersionUID = 1L;

                                                       @Override
                                                       boolean satisfiedBy(
                                                               final FileInfo fileInfo) {
                                                           return false;
                                                       }

                                                       @Override
                                                       boolean exactlySpecifies(
                                                               final FileInfo fileInfo) {
                                                           return false;
                                                       }

                                                       @Override
                                                       public String toString() {
                                                           return "NOTHING";
                                                       }

                                                       private Object readResolve() {
                                                           return NOTHING;
                                                       }
                                                   };
    /**
     * The constraints.
     */
    private final Set<Constraint> constraints      = new TreeSet<Constraint>();

    /**
     * Constructs from an array of constraints.
     * 
     * @param constraints
     *            The array of constraints.
     * @throws NullPointerException
     *             if {@code constraints == null}.
     */
    Filter(final Constraint[] constraints) {
        if (null == constraints) {
            throw new NullPointerException();
        }

        for (final Constraint constraint : constraints) {
            this.constraints.add(constraint);
        }
    }

    /**
     * Indicates if a file satisfies this filter.
     * 
     * @param fileInfo
     *            A description of the file.
     * @return {@code true} if and only if the file satisfies this filter.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    boolean satisfiedBy(final FileInfo fileInfo) {
        for (final Constraint constraint : constraints) {
            final Object value = fileInfo.getAttributeValue(constraint
                    .getAttribute());

            if (!constraint.satisfiedBy(value)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Indicates if this instance is satisfied by, and only by, a given file.
     * 
     * @param fileInfo
     *            The file in question.
     * @return {@code true} if and only if this filter is satisfied by, and only
     *         by, the given file.
     */
    boolean exactlySpecifies(final FileInfo fileInfo) {
        final Map<Attribute, Object> attrMap = fileInfo.getAttributeMap();

        if (attrMap.size() != constraints.size()) {
            return false;
        }

        for (final Constraint constraint : constraints) {
            if (!constraint.exactlySpecifies(attrMap.get(constraint
                    .getAttribute()))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int compareTo(final Filter that) {
        final Iterator<Constraint> thisIter = constraints.iterator();
        final Iterator<Constraint> thatIter = that.constraints.iterator();
        int status;

        for (;;) {
            if (thisIter.hasNext()) {
                if (thatIter.hasNext()) {
                    status = thisIter.next().compareTo(thatIter.next());

                    if (0 != status) {
                        return status;
                    }
                }
                else {
                    return 1;
                }
            }
            else {
                return thatIter.hasNext()
                        ? -1
                        : 0;
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((constraints == null)
                ? 0
                : constraints.hashCode());
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
        final Filter other = (Filter) obj;
        return 0 == compareTo(other);
    }

    private Object readResolve() {
        return new Filter(constraints
                .toArray(new Constraint[constraints.size()]));
    }
}
