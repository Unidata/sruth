package edu.ucar.unidata.dynaccn;

import java.lang.reflect.Field;

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */

/**
 * Utility string methods.
 * 
 * @author Steven R. Emmerson
 */
final class Strings {
    /**
     * Returns the string representation of an object.
     * 
     * @param obj
     *            The object to have its string representation returned.
     */
    static String toString(final Object obj) {
        final StringBuffer buf = new StringBuffer(obj.getClass()
                .getSimpleName());

        buf.append("{");

        final Field[] fields = obj.getClass().getDeclaredFields();

        for (int i = 0; i < fields.length; ++i) {
            if (0 < i) {
                buf.append(", ");
            }
            buf.append(fields[i].getName());
            buf.append("=");
            try {
                buf.append(toString(fields[i].get(obj)));
            }
            catch (final IllegalArgumentException e) {
            }
            catch (final IllegalAccessException e) {
            }
        }

        buf.append("}");

        return buf.toString();
    }

    static String toString(final Object obj, final Field[] fields) {
        final StringBuffer buf = new StringBuffer(obj.getClass()
                .getSimpleName());

        buf.append("{");

        for (int i = 0; i < fields.length; ++i) {
            if (0 < i) {
                buf.append(", ");
            }
            buf.append(fields[i].getName());
            buf.append("=");
            try {
                buf.append(toString(fields[i].get(obj)));
            }
            catch (final IllegalArgumentException e) {
            }
            catch (final IllegalAccessException e) {
            }
        }

        buf.append("}");

        return buf.toString();
    }
}
