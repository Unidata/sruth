/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A conjunction of constraints on the attributes of a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Filter {
    /**
     * The constraints.
     */
    private final Constraint[] constraints;

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

        this.constraints = constraints;
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
            final AttributeValue value = fileInfo.getAttributeValue(constraint
                    .getAttribute());

            if (!constraint.satisfiedBy(value)) {
                return false;
            }
        }

        return true;
    }
}
