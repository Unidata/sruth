import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucar.unidata.sruth.Misc;

/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */

/**
 * Experiments with the {@link Files#walkFileTree(Path, FileVisitor)} method.
 * 
 * @author Steven R. Emmerson
 */
public class WalkFileTreeExperiment {
    private static final Path TEST_DIR = Paths.get(
                                               System.getProperty("java.io.tmpdir"))
                                               .resolve(
                                                       WalkFileTreeExperiment.class
                                                               .getSimpleName());
    private static final Path SUBDIR   = TEST_DIR.resolve("subdir");
    private static final Path FILE     = SUBDIR.resolve("file");

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        assertEquals(0, Misc.system("rm", "-rf", TEST_DIR.toString()));
        assertTrue(TEST_DIR.toFile().mkdir());
        assertTrue(SUBDIR.toFile().mkdir());
        assertTrue(FILE.toFile().createNewFile());
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
    public final void testWalkRoot() throws IOException {
        final AtomicBoolean testDirVisited = new AtomicBoolean(false);
        final AtomicBoolean subdirVisited = new AtomicBoolean(false);
        final AtomicBoolean fileVisited = new AtomicBoolean(false);
        Files.walkFileTree(TEST_DIR, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir,
                    final BasicFileAttributes attributes) {
                if (dir.equals(TEST_DIR)) {
                    testDirVisited.compareAndSet(false, true);
                }
                else if (dir.equals(SUBDIR)) {
                    subdirVisited.compareAndSet(false, true);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path path,
                    final BasicFileAttributes attributes) throws IOException {
                assertTrue(attributes.isRegularFile());
                assertTrue(path.equals(FILE));
                fileVisited.compareAndSet(false, true);
                return FileVisitResult.CONTINUE;
            }
        });
        assertTrue(testDirVisited.get());
        assertTrue(subdirVisited.get());
        assertTrue(fileVisited.get());
    }

    @Test
    public final void testWalkFile() throws IOException {
        final AtomicBoolean fileVisited = new AtomicBoolean(false);
        Files.walkFileTree(FILE, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir,
                    final BasicFileAttributes attributes) {
                assertTrue(false);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path path,
                    final BasicFileAttributes attributes) throws IOException {
                assertTrue(attributes.isRegularFile());
                assertTrue(path.equals(FILE));
                fileVisited.compareAndSet(false, true);
                return FileVisitResult.CONTINUE;
            }
        });
        assertTrue(fileVisited.get());
    }
}
