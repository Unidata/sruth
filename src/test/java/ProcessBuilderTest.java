import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Tests the class {@link java.lang.ProcessBuilder}.
 * 
 * @author Steven R. Emmerson
 */
public class ProcessBuilderTest {

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test2ArgDate() throws IOException, InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder("date", "-u");
        builder.inheritIO();
        final Process process = builder.start();
        final int status = process.waitFor();
        assertTrue(status == 0);
    }

    @Test(expected = IOException.class)
    public void testSingleStringDate() throws IOException, InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder("date -u");
        builder.inheritIO();
        final Process process = builder.start();
        final int status = process.waitFor();
        assertTrue(status == 0);
    }
}
