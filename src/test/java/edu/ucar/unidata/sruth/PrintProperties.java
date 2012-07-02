/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Prints the system properties of the Java Virtual Machine (JVM).
 * 
 * @author Steven R. Emmerson
 */
final class PrintProperties {
    public static void main(final String[] args) {
        final Properties properties = System.getProperties();
        final SortedSet<Object> sortedNames = new TreeSet<Object>(
                properties.keySet());
        for (final Object name : sortedNames) {
            System.out.println(name.toString() + "="
                    + properties.getProperty((String) name));
        }
    }
}
