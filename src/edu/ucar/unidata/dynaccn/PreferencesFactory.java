/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.util.prefs.Preferences;

/**
 * Static factory for constructing class-specific (as opposed to
 * package-specific) {@link Preferences} nodes.
 * 
 * @author Steven R. Emmerson
 */
final class PreferencesFactory {
    /**
     * Returns the {@link Preferences} node associated with a class.
     * 
     * @param parent
     *            The parent (package) {@link Preferences} node of the class.
     * @param c
     *            The class
     * @return The {@link Preferences} node associated with the class.
     */
    private static Preferences nodeForClass(final Preferences parent,
            final Class<?> c) {
        return parent.node(c.getSimpleName());
    }

    /**
     * Returns the system {@link Preferences} node associated with a class.
     * 
     * @param c
     *            The class
     * @return The system {@link Preferences} node associated with the class.
     */
    static Preferences systemNodeForClass(final Class<?> c) {
        return nodeForClass(Preferences.systemNodeForPackage(c), c);
    }

    /**
     * Returns the user {@link Preferences} node associated with a class.
     * 
     * @param c
     *            The class
     * @return The user {@link Preferences} node associated with the class.
     */
    static Preferences userNodeForClass(final Class<?> c) {
        return nodeForClass(Preferences.userNodeForPackage(c), c);
    }
}
