import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucar.unidata.sruth.Misc;

/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Tests file creation time.
 * 
 * @author Steven R. Emmerson
 */
public class FileModificationTimeTest {
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
    public void testModificationTime() throws Exception {
        Path path;
        BasicFileAttributeView view;
        BasicFileAttributes attributes;
        FileTime time1;
        FileTime time2;

        path = Paths.get(System.getProperty("java.io.tmpdir"));
        assertNotNull(path);
        assertTrue(Files.exists(path));
        view = Files.getFileAttributeView(path, BasicFileAttributeView.class);
        assertNotNull(view);
        attributes = view.readAttributes();
        assertNotNull(attributes);
        time1 = attributes.lastModifiedTime();
        assertNotNull(time1);

        time2 = (FileTime) Files.getAttribute(path, "lastModifiedTime");
        assertNotNull(time2);
        assertEquals(time1, time2);
        final Path testDir = Paths.get("/tmp/FileModificationTimeTest");
        assertEquals(0, Misc.system("rm", "-rf", testDir.toString()));
        Files.createDirectory(testDir);
        path = testDir.resolve("testFile");
        Files.createFile(path);
        assertTrue(Files.exists(path));
        view = Files.getFileAttributeView(path, BasicFileAttributeView.class);
        assertNotNull(view);
        attributes = view.readAttributes();
        assertNotNull(attributes);
        time1 = attributes.lastModifiedTime();
        assertNotNull(time1);
        time2 = (FileTime) Files.getAttribute(path, "lastModifiedTime");
        assertNotNull(time2);
        assertEquals(time1, time2);
        Thread.sleep(2000);
        final Path newPath = testDir.resolve("newFile");
        Files.move(path, newPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        assertFalse(Files.exists(path));
        assertTrue(Files.exists(newPath));
        assertEquals(time1, Files.getAttribute(newPath, "lastModifiedTime"));
        assertEquals(0, Misc.system("rm", "-rf", testDir.toString()));
    }
}
