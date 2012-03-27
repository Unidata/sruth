import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Tests file access.
 * 
 * @author Steven R. Emmerson
 */
public class FileAccessTest {
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
     * Tests the exception that occurs when a necessary parent-directory doesn't
     * exist.
     * 
     * @throws IOException
     */
    @Test(expected = NoSuchFileException.class)
    public void testMissingDirectory() throws IOException {
        Files.createFile(Paths.get("/tmp/nonExistantDirectory/nonExistantFile"));
    }

    /**
     * Tests an empty directory.
     * 
     * @throws IOException
     */
    @Test
    public void testEmptyDirectory() throws IOException {
        final Path dir = Paths.get(System.getProperty("java.io.tmpdir"))
                .resolve(getClass().getSimpleName());
        Files.deleteIfExists(dir);
        Files.createDirectory(dir);
        final DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
        try {
            assertFalse(stream.iterator().hasNext());
        }
        finally {
            stream.close();
            Files.delete(dir);
        }
    }
}
