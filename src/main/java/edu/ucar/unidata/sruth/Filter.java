/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import net.jcip.annotations.Immutable;
import edu.ucar.unidata.sruth.Connection.Message;

/**
 * A filter for selecting a class of files.
 * <p>
 * The {@link #compareTo(Filter)} and {@link #includes(Filter)} methods have the
 * following relationships:
 * 
 * <pre>
 * Filter A, B;
 * A.includes(B) => A.compareTo(B) <= 0
 * A.includes(B) && B.includes(A) <=> A.compareTo(B) == 0
 * </pre>
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
@Immutable
public class Filter implements Comparable<Filter>, Message {
    /**
     * The serial version ID.
     */
    private static final long        serialVersionUID = 1L;
    /**
     * The filter that matches everything.
     */
    public static final Filter       EVERYTHING       = new Filter(
                                                              new String[0]) {
                                                          /**
                                                           * The serial version
                                                           * ID.
                                                           */
                                                          private static final long serialVersionUID = 1L;

                                                          @Override
                                                          boolean matches(
                                                                  final ArchivePath path) {
                                                              return true;
                                                          }

                                                          @Override
                                                          boolean matchesOnly(
                                                                  final ArchivePath archivePath) {
                                                              return false;
                                                          }

                                                          @Override
                                                          boolean includes(
                                                                  final Filter filter) {
                                                              return true;
                                                          }

                                                          @Override
                                                          public int compareTo(
                                                                  final Filter that) {
                                                              return (this == that)
                                                                      ? 0
                                                                      : -1;
                                                          }

                                                          @Override
                                                          public String toString() {
                                                              return "EVERYTHING";
                                                          }
                                                      };
    /**
     * The filter that matches nothing.
     */
    public static final Filter       NOTHING          = new Filter(
                                                              (String[]) null) {
                                                          /**
                                                           * The serial version
                                                           * ID.
                                                           */
                                                          private static final long serialVersionUID = 1L;

                                                          @Override
                                                          boolean matches(
                                                                  final ArchivePath path) {
                                                              return false;
                                                          }

                                                          @Override
                                                          boolean matchesOnly(
                                                                  final ArchivePath archivePath) {
                                                              return false;
                                                          }

                                                          @Override
                                                          boolean includes(
                                                                  final Filter filter) {
                                                              return false;
                                                          }

                                                          @Override
                                                          public int compareTo(
                                                                  final Filter that) {
                                                              return (this == that)
                                                                      ? 0
                                                                      : 1;
                                                          }

                                                          @Override
                                                          public String toString() {
                                                              return "NOTHING";
                                                          }
                                                      };
    /**
     * The glob pattern for matching pathnames.
     * 
     * @serial
     */
    private final String             glob;
    /**
     * The broken-out components of the glob pattern.
     */
    private final transient String[] components;
    /**
     * Whether or not the glob pattern contains a metacharacter.
     */
    private final transient boolean  containsMeta;

    /**
     * Returns an instance corresponding to a glob pattern. The only valid
     * metacharacter is "*", which matches zero or more characters of a name
     * component without crossing directory boundaries.
     * 
     * @param glob
     *            The the glob pattern or {@code null}, in which case
     *            {@link #NOTHING} is returned.
     * @return A filter corresponding to the glob pattern.
     * @throws IllegalArgumentException
     *             if {@code glob.length > 1 && glob.charAt(0) == }
     *             {@link ArchivePath#SEPARATOR}.
     * @throws NullPointerException
     *             if {@code glob == null}.
     */
    public static Filter getInstance(final String glob) {
        if (glob == null) {
            return NOTHING;
        }
        String[] components = glob.split(ArchivePath.SEPARATOR);
        components = canonicalize(components);
        return components.length == 0
                ? EVERYTHING
                : new Filter(components);
    }

    /**
     * Returns the canonical form of an array of glob pattern components.
     * 
     * @param components
     *            The array of glob pattern components.
     * @return The canonical form of the glob pattern components. Might be the
     *         same array as {@code components}.
     */
    private static String[] canonicalize(final String[] components) {
        int i = components.length;
        while (--i >= 0) {
            if (!components[i].equals("*")) {
                break;
            }
        }
        if (i == components.length - 1) {
            return components;
        }
        final String[] newComponents = new String[i + 1];
        for (int j = 0; j <= i; j++) {
            newComponents[j] = components[j];
        }
        return newComponents;
    }

    /**
     * Constructs from the components of a glob pattern. The only valid
     * metacharacter is "*", which matches zero or more characters of a name
     * component up to the next directory boundary or the end of string
     * (whichever comes first).
     * 
     * @param components
     *            The components of the glob pattern or {@code null}, in which
     *            case the file will match nothing.
     * @throws IllegalArgumentException
     *             if one of the component elements is the empty string.
     * @throws IllegalArgumentException
     *             if one of the component elements contains the character '*'
     *             plus other characters.
     */
    private Filter(final String[] components) {
        if (components == null) {
            this.containsMeta = false;
            this.glob = null;
            this.components = null;
        }
        else {
            final StringBuilder buf = new StringBuilder();
            boolean containsMeta = false;
            for (int i = 0; i < components.length; i++) {
                final String component = components[i];
                if (component.length() == 0) {
                    throw new IllegalArgumentException();
                }
                if (component.contains("*")) {
                    if (component.length() == 1) {
                        containsMeta = true;
                    }
                    else {
                        throw new IllegalArgumentException();
                    }
                }
                if (i > 0) {
                    buf.append(ArchivePath.SEPARATOR);
                }
                buf.append(component);
            }
            this.containsMeta = containsMeta;
            this.glob = buf.toString();
            this.components = components;
        }
    }

    /**
     * Indicates if a file satisfies this filter.
     * 
     * @param archivePath
     *            Pathname of the file in question.
     * @return {@code true} if and only if the file satisfies this filter.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    boolean matches(final ArchivePath archivePath) {
        if (archivePath.getNameCount() < components.length) {
            return false;
        }
        for (int i = 0; i < components.length; i++) {
            if (!components[i].equals("*")
                    && !components[i].equals(archivePath.getName(i).toString())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Indicates if this instance is satisfied by, and only by, a given file.
     * 
     * @param archivePath
     *            Archive pathname of the file in question.
     * @return {@code true} if and only if this filter is satisfied by, and only
     *         by, the given file.
     */
    boolean matchesOnly(final ArchivePath archivePath) {
        if (containsMeta) {
            return false;
        }
        if (archivePath.getNameCount() != components.length) {
            return false;
        }
        for (int i = 0; i < components.length; i++) {
            if (!components[i].equals(archivePath.getName(i).toString())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Indicates if this instance includes another instance. Instance A includes
     * instance B if {@code B.matches(path)} implies {@code A.matches(path)} for
     * all paths.
     * 
     * @param that
     *            The other instance
     * @return {@code} if and only if this instance includes the other instance.
     * @throws NullPointerException
     *             if {@code that == null}.
     */
    boolean includes(final Filter that) {
        if (that == EVERYTHING) {
            return false;
        }
        if (that == NOTHING) {
            return false;
        }
        if (components.length > that.components.length) {
            return false;
        }
        for (int i = 0; i < components.length; i++) {
            if (!components[i].equals(that.components[i])
                    && !components[i].equals("*")) {
                return false;
            }
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((glob == null)
                ? 0
                : glob.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
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
        return compareTo(other) == 0;
    }

    /**
     * If A, B, and C are filters, it's possible to have A.includes(C),
     * !B.includes(C), and !A.includes(B) and yet A.compareTo(B) < 0 and
     * B.compareTo(C) < 0.
     */
    @Override
    public int compareTo(final Filter that) {
        if (this == that) {
            return 0;
        }
        if (glob == null) {
            return that.glob == null
                    ? 0
                    : 1;
        }
        if (that.glob == null) {
            return -1;
        }
        for (int i = 0; i < components.length; i++) {
            if (i >= that.components.length) {
                return 1;
            }
            if (!components[i].equals(that.components[i])) {
                return components[i].equals("*")
                        ? -1
                        : that.components[i].equals("*")
                                ? 1
                                : components[i].compareTo(that.components[i]);
            }
        }
        return (components.length == that.components.length)
                ? 0
                : -1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Filter [glob=" + glob + "]";
    }

    private Object readResolve() {
        return getInstance(glob);
    }
}
