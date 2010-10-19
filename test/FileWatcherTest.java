import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKind;
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

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
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
         * Pathname of the directory to watch.
         */
        private final Path rootDir;

        /**
         * Constructs from the pathname of the directory to watch.
         * 
         * @param rootDir
         *            Pathname of the directory to watch.
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        FileWatcher(final Path rootDir) {
            if (rootDir == null) {
                throw new NullPointerException();
            }
            this.rootDir = rootDir;
        }

        /**
         * Executes this instance.
         */
        public Void call() throws InterruptedException, IOException {
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(toString());
            try {
                final WatchService watchService = FileSystems.getDefault()
                        .newWatchService();
                try {
                    rootDir.register(watchService,
                            StandardWatchEventKind.ENTRY_CREATE,
                            StandardWatchEventKind.ENTRY_DELETE);
                    for (;;) {
                        final WatchKey key = watchService.take();
                        for (final WatchEvent<?> event : key.pollEvents()) {
                            final WatchEvent.Kind<?> kind = event.kind();
                            if (kind == StandardWatchEventKind.OVERFLOW) {
                                System.err.println("Couldn't keep-up: " + this);
                            }
                            else {
                                final Path path = rootDir.resolve((Path) event
                                        .context());
                                if (kind == StandardWatchEventKind.ENTRY_CREATE) {
                                    created(path);
                                }
                                else if (kind == StandardWatchEventKind.ENTRY_DELETE) {
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
            return "FileWatcher [rootDir=" + rootDir + "]";
        }
    }

    private static Future<Void>                    fileWatcherFuture;
    private static ExecutorService                 executor          = Executors
                                                                             .newCachedThreadPool();
    private static ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                             executor);
    private static final Path                      ROOT_DIR          = Paths
                                                                             .get("/tmp/FileWatcherTest");
    private static final Path                      FILE              = ROOT_DIR
                                                                             .resolve(Paths
                                                                                     .get("file"));
    private static final Path                      SUBDIR            = ROOT_DIR
                                                                             .resolve(Paths
                                                                                     .get("subdir"));
    private static final Path                      SUBFILE           = SUBDIR
                                                                             .resolve(Paths
                                                                                     .get("file"));
    private static final int                       SLEEP             = 5000;

    private volatile boolean                       fileExists        = false;
    private volatile boolean                       subdirExists      = false;
    private volatile boolean                       subFileExists     = false;

    private static int system(final String... cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        return process.waitFor();
    }

    private static void removeRootDir() throws IOException,
            InterruptedException {
        system("rm", "-rf", ROOT_DIR.toString());
    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeRootDir();
        system("mkdir", ROOT_DIR.toString());
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        removeRootDir();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        final FileWatcher fileWatcher = new FileWatcher(ROOT_DIR);
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
        Assert.assertFalse(fileWatcherFuture.isDone());
        Assert.assertEquals(0, system("touch", FILE.toString()));
        Thread.sleep(SLEEP);
        Assert.assertTrue(fileExists);
        Assert.assertEquals(0, system("mkdir", SUBDIR.toString()));
        Thread.sleep(SLEEP);
        Assert.assertTrue(subdirExists);
        Assert.assertEquals(0, system("touch", SUBFILE.toString()));
        Thread.sleep(SLEEP);
        Assert.assertTrue(subFileExists);
    }

    void created(final Path path) {
        System.out.println("Created: " + path);
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
        System.out.println("Removed: " + path);
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
