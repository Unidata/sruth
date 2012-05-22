/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link XmlActionFile}.
 * 
 * @author Steven R. Emmerson
 */
public class XmlActionFileTest {

    private ExecutorService executor;

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
        executor = Executors.newCachedThreadPool();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test(expected = NullPointerException.class)
    public void testNullPath() throws IOException {
        XmlActionFile.getProcessor((URL) null);
    }

    @Test(expected = FileNotFoundException.class)
    public void testNonExistantFile() throws IOException {
        XmlActionFile.getProcessor(new URL("file:///non-existant-file"));
    }

    @Test(expected = IOException.class)
    public void testBadXml() throws IOException, InterruptedException {
        XmlActionFile.getProcessor("");
    }

    @Test(expected = IOException.class)
    public void testNoActions() throws IOException {
        XmlActionFile.getProcessor("<?xml version=\"1.0\"?>");
    }

    @Test
    public void testFileAction() throws IOException, InterruptedException {
        // Get the data-product processor
        final Path destDir = Paths.get("/tmp/XmlActionFileTest");
        final Processor processor = XmlActionFile
                .getProcessor("<?xml version=\"1.0\"?>" + "<actions>"
                        + "<entry pattern=\"(.*)\">" + "<file path=\""
                        + destDir.toString() + "/$1\"/>" + "</entry>"
                        + "</actions>");
        assertNotNull(processor);
        executor.submit(processor);
        // Set up the source and destination
        final Path srcDir = Paths.get("/home/steve");
        final Path name = Paths.get(".shrc");
        final Path srcPath = srcDir.resolve(name);
        final Path destPath = destDir.resolve(name);
        Files.deleteIfExists(destPath);
        Files.deleteIfExists(destDir);
        final FileInfo fileInfo = new FileInfo(
                new FileId(new ArchivePath(name)), (Long) Files.getAttribute(
                        srcPath, "size"));
        final DataProduct dataProduct = new DataProduct(srcDir, fileInfo);
        // Process the data-product
        assertTrue(processor.offer(dataProduct));
        Thread.sleep(200);
        assertTrue(Files.exists(destPath));
        final int status = Misc.system("cmp", srcPath.toString(),
                destPath.toString());
        assertEquals(0, status);
        // Cleanup
        Files.delete(destPath);
        Files.delete(destDir);
    }
}
