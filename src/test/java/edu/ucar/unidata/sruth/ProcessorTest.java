/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the {@link Processor} class.
 * 
 * @author Steven R. Emmerson
 */
public class ProcessorTest {

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

    /**
     * Test method for {@link edu.ucar.unidata.sruth.Processor#Processor()}.
     */
    @Test
    public final void testProcessor() {
        new Processor();
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Processor#add(Pattern, Action)} .
     */
    @Test
    public final void testAdd() {
        final Processor processor = new Processor();
        final Pattern pattern = Pattern.compile("(.*)");
        final Action action = new FileAction("/tmp/ProcessorTest/$1");
        processor.add(pattern, action);
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Processor#process(edu.ucar.unidata.sruth.DataProduct)}
     * .
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public final void testProcess() throws IOException, InterruptedException {
        final Processor processor = new Processor();
        final Path srcDir = Paths.get("/home/steve");
        final Path destDir = Paths.get("/tmp/ProcessorTest");
        final Path name = Paths.get(".shrc");
        final Action action = new FileAction(destDir.toString() + "/$1");
        final Pattern pattern = Pattern.compile("(.*)");
        processor.add(pattern, action);
        final FileInfo fileInfo = new FileInfo(
                new FileId(new ArchivePath(name)), (Long) Files.getAttribute(
                        srcDir.resolve(name), "size"));
        final DataProduct dataProduct = new DataProduct(srcDir, fileInfo);
        assertTrue(processor.process(dataProduct));
        assertTrue(Files.exists(destDir.resolve(name)));
        final int status = Misc.system("cmp", srcDir.resolve(name).toString(),
                destDir.resolve(name).toString());
        assertEquals(0, status);
        Files.delete(destDir.resolve(name));
        Files.delete(destDir);
    }

}
