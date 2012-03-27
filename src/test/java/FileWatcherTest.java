import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;
import net.jcip.annotations.NotThreadSafe;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucar.unidata.sruth.Misc;

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Tests if watching a directory also watches subdirectories.
 * 
 * @author Steven R. Emmerson
 */
public class FileWatcherTest {
    /**
     * Watches for newly-created files.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @NotThreadSafe
    private final class FileWatcher implements Callable<Void> {
        /**
         * Executes this instance.
         */
        public Void call() throws InterruptedException, IOException {
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(toString());
            try {
                final WatchService watchService = ROOT_DIR.getFileSystem()
                        .newWatchService();
                assertNotNull(watchService);
                try {
                    ROOT_DIR.register(watchService,
                            StandardWatchEventKinds.ENTRY_CREATE,
                            StandardWatchEventKinds.ENTRY_DELETE);
                    for (;;) {
                        final WatchKey key = watchService.take();
                        for (final WatchEvent<?> event : key.pollEvents()) {
                            final WatchEvent.Kind<?> kind = event.kind();
                            if (kind == StandardWatchEventKinds.OVERFLOW) {
                                System.err.println("Couldn't keep-up: " + this);
                            }
                            else {
                                final Path path = ROOT_DIR.resolve((Path) event
                                        .context());
                                // System.out.println("Path: " + path);
                                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                    created(path);
                                }
                                else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                    removed(path);
                                }
                            }
                        }
                        if (!key.reset()) {
                            System.err.println("Couldn't reset watch-key: "
                                    + this);
                        }
                    }
                }
                finally {
                    watchService.close();
                }
            }
            finally {
                Thread.currentThread().setName(origThreadName);
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "FileWatcher [rootDir=" + ROOT_DIR + "]";
        }
    }

    private static Future<Void>                    fileWatcherFuture;
    private static ExecutorService                 executor          = Executors
                                                                             .newCachedThreadPool();
    private static ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                             executor);
    private static final Path                      ROOT_DIR          = Paths.get("/tmp/FileWatcherTest");
    private static final Path                      FILE              = ROOT_DIR
                                                                             .resolve("file");
    private static final Path                      SUBDIR            = ROOT_DIR
                                                                             .resolve("subdir");
    private static final Path                      SUBFILE           = SUBDIR.resolve("subfile");
    private static final int                       SLEEP             = 20;

    private volatile boolean                       fileExists        = false;
    private volatile boolean                       subdirExists      = false;
    private volatile boolean                       subFileExists     = false;

    private static void removeRootDir() throws IOException,
            InterruptedException {
        Misc.system("rm", "-rf", ROOT_DIR.toString());
    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeRootDir();
        assertEquals(0, Misc.system("mkdir", ROOT_DIR.toString()));
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // removeRootDir();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        final FileWatcher fileWatcher = new FileWatcher();
        fileWatcherFuture = completionService.submit(fileWatcher);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void test() throws IOException, InterruptedException {
        Thread.sleep(SLEEP);
        Assert.assertFalse(fileWatcherFuture.isDone());
        assertTrue(FILE.toFile().createNewFile());
        Thread.sleep(SLEEP);
        Assert.assertTrue(fileExists);
        assertTrue(SUBDIR.toFile().mkdir());
        Thread.sleep(SLEEP);
        Assert.assertTrue(subdirExists);
        assertTrue(SUBFILE.toFile().createNewFile());
        Thread.sleep(SLEEP);
        Assert.assertFalse(subFileExists);
    }

    void created(final Path path) {
        // System.out.println("Created: " + path);
        if (path.equals(FILE)) {
            fileExists = true;
        }
        else if (path.equals(SUBDIR)) {
            subdirExists = true;
        }
        else if (path.equals(SUBFILE)) {
            subFileExists = true;
        }
    }

    void removed(final Path path) {
        // System.out.println("Removed: " + path);
        if (path.equals(FILE)) {
            fileExists = false;
        }
        if (path.equals(SUBDIR)) {
            subdirExists = false;
        }
        else if (path.equals(SUBFILE)) {
            subFileExists = false;
        }
    }
}
