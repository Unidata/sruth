import java.io.IOException;
import java.nio.file.DirectoryStream;
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
 * reserved.  See file LICENSE in the top-level source directory for licensing
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
        Paths.get("/tmp/nonExistantDirectory/nonExistantFile").createFile();
    }

    /**
     * Tests an empty directory.
     * 
     * @throws IOException
     */
    @Test
    public void testEmptyDirectory() throws IOException {
        final Path dir = Paths.get("/tmp/" + getClass().getSimpleName());
        dir.createDirectory();
        final DirectoryStream<Path> stream = dir.newDirectoryStream();
        try {
            for (final Path path : stream) {
                System.out.println(path.toString());
            }
        }
        finally {
            stream.close();
            dir.delete();
        }
    }
}
