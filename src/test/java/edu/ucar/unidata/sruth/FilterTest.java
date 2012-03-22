/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Test;

/**
 * @author Steven R. Emmerson
 */
public class FilterTest {
    private static Filter      fooFilter        = Filter.getInstance("foo");
    private static Filter      barFilter        = Filter.getInstance("bar");
    private static Filter      fooSubFilter     = Filter.getInstance("foo/*");
    private static Filter      subFilter        = Filter.getInstance("*/sub");
    private static Filter      fooSubBarFilter  = Filter.getInstance("foo/sub/bar");
    private static Filter      fooStarBarFilter = Filter.getInstance("foo/*/bar");

    private static ArchivePath fooPath          = new ArchivePath("foo");
    private static ArchivePath barPath          = new ArchivePath("bar");
    private static ArchivePath foobarPath       = new ArchivePath("foobar");
    private static ArchivePath fooSubPath       = new ArchivePath("foo/sub");
    private static ArchivePath fooSubBarPath    = new ArchivePath("foo/sub/bar");

    @Test
    public final void testEquals() {
        assertTrue(Filter.EVERYTHING.equals(Filter.EVERYTHING));
        assertTrue(!Filter.EVERYTHING.equals(Filter.NOTHING));
        assertTrue(Filter.NOTHING.equals(Filter.NOTHING));
        assertTrue(!Filter.NOTHING.equals(Filter.EVERYTHING));

        assertTrue(fooFilter.equals(fooFilter));
        assertTrue(!fooFilter.equals(barFilter));
        assertTrue(fooFilter.equals(fooSubFilter));
        assertTrue(!fooFilter.equals(subFilter));
        assertTrue(!fooFilter.equals(fooStarBarFilter));

        assertTrue(!barFilter.equals(fooFilter));
        assertTrue(barFilter.equals(barFilter));
        assertTrue(!barFilter.equals(fooSubFilter));
        assertTrue(!barFilter.equals(subFilter));
        assertTrue(!barFilter.equals(fooSubBarFilter));
        assertTrue(!barFilter.equals(fooStarBarFilter));

        assertTrue(fooSubFilter.equals(fooFilter));
        assertTrue(!fooSubFilter.equals(barFilter));
        assertTrue(fooSubFilter.equals(fooSubFilter));
        assertTrue(!fooSubFilter.equals(subFilter));
        assertTrue(!fooSubFilter.equals(fooSubBarFilter));
        assertTrue(!fooSubFilter.equals(fooStarBarFilter));

        assertTrue(!subFilter.equals(fooFilter));
        assertTrue(!subFilter.equals(barFilter));
        assertTrue(!subFilter.equals(fooSubFilter));
        assertTrue(subFilter.equals(subFilter));
        assertTrue(!subFilter.equals(fooSubBarFilter));
        assertTrue(!subFilter.equals(fooStarBarFilter));

        assertTrue(!fooSubBarFilter.equals(fooFilter));
        assertTrue(!fooSubBarFilter.equals(barFilter));
        assertTrue(!fooSubBarFilter.equals(fooSubFilter));
        assertTrue(!fooSubBarFilter.equals(subFilter));
        assertTrue(fooSubBarFilter.equals(fooSubBarFilter));
        assertTrue(!fooSubBarFilter.equals(fooStarBarFilter));

        assertTrue(!fooStarBarFilter.equals(fooFilter));
        assertTrue(!fooStarBarFilter.equals(barFilter));
        assertTrue(!fooStarBarFilter.equals(fooSubFilter));
        assertTrue(!fooStarBarFilter.equals(subFilter));
        assertTrue(!fooStarBarFilter.equals(fooSubBarFilter));
        assertTrue(fooStarBarFilter.equals(fooStarBarFilter));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Filter#matches(edu.ucar.unidata.sruth.ArchivePath)}
     * .
     */
    @Test
    public final void testMatches() {
        assertTrue(Filter.EVERYTHING.matches(fooPath));
        assertTrue(fooFilter.matches(fooPath));
        assertTrue(fooFilter.matches(fooSubPath));
        assertTrue(fooFilter.matches(fooSubBarPath));
        assertTrue(fooSubFilter.matches(fooSubPath));
        assertTrue(subFilter.matches(fooSubPath));
        assertTrue(fooStarBarFilter.matches(fooSubBarPath));
        assertTrue(fooSubFilter.matches(fooPath));
        assertTrue(fooSubBarFilter.matches(fooSubBarPath));

        assertFalse(Filter.NOTHING.matches(fooPath));
        assertFalse(fooFilter.matches(barPath));
        assertFalse(fooFilter.matches(foobarPath));
        assertFalse(barFilter.matches(fooPath));
        assertFalse(barFilter.matches(fooSubPath));
        assertFalse(barFilter.matches(fooSubBarPath));
        assertFalse(fooStarBarFilter.matches(fooSubPath));
        assertFalse(fooSubBarFilter.matches(fooPath));
        assertFalse(fooSubBarFilter.matches(fooSubPath));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Filter#matchesOnly(edu.ucar.unidata.sruth.ArchivePath)}
     * .
     */
    @Test
    public final void testMatchesOnly() {
        assertTrue(fooFilter.matchesOnly(fooPath));
        assertTrue(fooSubFilter.matchesOnly(fooPath));
        assertTrue(fooSubBarFilter.matchesOnly(fooSubBarPath));

        assertFalse(fooFilter.matchesOnly(barPath));
        assertFalse(fooFilter.matchesOnly(fooSubPath));
        assertFalse(fooFilter.matchesOnly(fooSubBarPath));
        assertFalse(fooSubFilter.matchesOnly(barPath));
        assertFalse(fooSubFilter.matchesOnly(fooSubPath));
        assertFalse(fooSubFilter.matchesOnly(fooSubBarPath));
        assertFalse(subFilter.matchesOnly(barPath));
        assertFalse(subFilter.matchesOnly(fooSubPath));
        assertFalse(subFilter.matchesOnly(fooSubBarPath));
        assertFalse(fooStarBarFilter.matchesOnly(fooPath));
        assertFalse(fooStarBarFilter.matchesOnly(fooSubPath));
        assertFalse(fooStarBarFilter.matchesOnly(fooSubBarPath));
        assertFalse(fooSubBarFilter.matchesOnly(fooPath));
        assertFalse(fooSubBarFilter.matchesOnly(fooSubPath));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Filter#includes(edu.ucar.unidata.sruth.Filter)}
     * .
     */
    @Test
    public final void testIncludes() {
        assertTrue(fooFilter.includes(fooFilter));
        assertTrue(fooFilter.includes(fooSubFilter));
        assertTrue(fooSubFilter.includes(fooFilter));
        assertTrue(fooFilter.includes(fooStarBarFilter));
        assertTrue(fooSubFilter.includes(fooSubFilter));
        assertTrue(fooStarBarFilter.includes(fooStarBarFilter));
        assertTrue(fooSubBarFilter.includes(fooSubBarFilter));
        assertTrue(fooStarBarFilter.includes(fooSubBarFilter));

        assertFalse(fooFilter.includes(barFilter));
        assertFalse(fooFilter.includes(subFilter));
        assertFalse(subFilter.includes(fooFilter));
        assertFalse(subFilter.includes(fooStarBarFilter));
        assertFalse(fooStarBarFilter.includes(fooFilter));
        assertFalse(fooStarBarFilter.includes(subFilter));
        assertFalse(fooSubBarFilter.includes(fooFilter));
        assertFalse(fooSubBarFilter.includes(subFilter));
        assertFalse(fooSubBarFilter.includes(fooStarBarFilter));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Filter#compareTo(edu.ucar.unidata.sruth.Filter)}
     * .
     */
    @Test
    public final void testCompareTo() {
        assertTrue(fooFilter.compareTo(fooFilter) == 0);
        assertTrue(fooFilter.compareTo(barFilter) != 0);
        assertTrue(fooFilter.compareTo(fooSubFilter) == 0);
        assertTrue(fooFilter.compareTo(subFilter) != 0);
        assertTrue(fooFilter.compareTo(fooSubBarFilter) != 0);
        assertTrue(fooFilter.compareTo(fooStarBarFilter) != 0);

        assertTrue(fooSubFilter.compareTo(fooSubFilter) == 0);
        assertTrue(fooSubFilter.compareTo(fooFilter) == 0);
        assertTrue(fooSubFilter.compareTo(subFilter) != 0);
        assertTrue(fooSubFilter.compareTo(fooSubBarFilter) != 0);
        assertTrue(fooSubFilter.compareTo(fooStarBarFilter) != 0);

        assertTrue(subFilter.compareTo(fooFilter) != 0);
        assertTrue(subFilter.compareTo(subFilter) == 0);
        assertTrue(subFilter.compareTo(fooSubBarFilter) != 0);
        assertTrue(subFilter.compareTo(fooStarBarFilter) != 0);

        assertTrue(fooSubBarFilter.compareTo(fooFilter) != 0);
        assertTrue(fooSubBarFilter.compareTo(subFilter) != 0);
        assertTrue(fooSubBarFilter.compareTo(fooSubBarFilter) == 0);
        assertTrue(fooSubBarFilter.compareTo(fooStarBarFilter) != 0);

        assertTrue(fooStarBarFilter.compareTo(fooFilter) != 0);
        assertTrue(fooStarBarFilter.compareTo(subFilter) != 0);
        assertTrue(fooStarBarFilter.compareTo(fooSubBarFilter) != 0);
        assertTrue(fooStarBarFilter.compareTo(fooStarBarFilter) == 0);
    }

    private final Object test(final Serializable obj) throws IOException,
            ClassNotFoundException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        final ByteArrayInputStream bais = new ByteArrayInputStream(
                baos.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(bais);
        final Object actual = ois.readObject();
        assertNotNull(actual);
        assertTrue(actual.equals(obj));
        return actual;
    }

    private final void testExact(final Serializable obj) throws IOException,
            ClassNotFoundException {
        final Object actual = test(obj);
        assertTrue(actual == obj);
    }

    @Test
    public final void testSerialization() throws IOException,
            ClassNotFoundException {
        testExact(Filter.NOTHING);
        testExact(Filter.EVERYTHING);
        test(fooFilter);
        test(barFilter);
        test(fooSubFilter);
        test(subFilter);
        test(fooSubBarFilter);
        test(fooStarBarFilter);
    }
}
