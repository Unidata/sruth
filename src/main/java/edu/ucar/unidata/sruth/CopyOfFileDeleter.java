/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import net.jcip.annotations.NotThreadSafe;

import org.slf4j.Logger;

/**
 * Deletes files that are descendants of a root-directory according to their
 * time-to-live.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class CopyOfFileDeleter implements Callable<Void> {
    /**
     * The logger for this class.
     */
    private static Logger        logger       = Util.getLogger();
    /**
     * The file-deletion queue.
     */
    private final PathDelayQueue queue;
    /**
     * The number of deleted files.
     */
    private volatile long        deletedCount = 0;
    /**
     * The root-directory.
     */
    private final Path           rootDir;

    /**
     * Constructs from the root-directory and the file-deletion queue.
     * 
     * @param rootDir
     *            The root-directory.
     * @param queue
     *            The file-deletion queue.
     * @throws NullPointerException
     *             if {@code rootDir == null || queue == null}.
     */
    CopyOfFileDeleter(final Path rootDir, final PathDelayQueue queue) {
        if (rootDir == null || queue == null) {
            throw new NullPointerException();
        }
        this.rootDir = rootDir;
        this.queue = queue;
    }

    /**
     * Accepts responsibility for deleting a file at the appropriate time. If
     * the appropriate time is not in the future, then the file is immediately
     * deleted.
     * 
     * @param path
     *            Pathname of the file relative to the root-directory.
     * @param time
     *            When the file should be deleted in milliseconds from now.
     * @throws IllegalArgumentException
     *             if the path isn't a descendant of the root-directory.
     * @throws IOException
     *             if an I/O error occurs.
     * @see #call()
     */
    void delete(final Path path, final long time) throws IOException {
        if (!path.startsWith(rootDir)) {
            throw new IllegalArgumentException(
                    "Path not descendant of root-directory: path=\"" + path
                            + "\", rootDir=\"" + rootDir + "\"");
        }
        if (time <= 0) {
            delete(path);
        }
        else {
            queue.add(path, System.currentTimeMillis() + time);
        }
    }

    /**
     * Returns the number of pending deletions.
     * 
     * @return The number of pending deletions.
     */
    int getPendingCount() {
        return queue.size();
    }

    /**
     * Returns the number of deleted files.
     * 
     * @return The number of deleted files.
     */
    long getDeletedCount() {
        return deletedCount;
    }

    /**
     * Executes this instance. Doesn't return. The following actions are
     * repeatedly executed: 1) the head of the queue is retrieved (but not
     * removed); 2) the associated file is deleted; and 3) the associated entry
     * is removed from the queue. Thus, the queue might contain an already
     * deleted entry if, for example, it is implemented using a persistent file
     * and a power failure occurs.
     * 
     * The ancestor directories of a deleted file are deleted when they become
     * empty.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Override
    public Void call() throws InterruptedException, IOException {
        for (;;) {
            final Path path = queue.peek();
            delete(path);
            final Path removedPath = queue.remove();
            assert path.equals(removedPath) : "Different paths: " + path
                    + " != " + removedPath;
        }
    }

    /**
     * Waits until the queue of pending deletions is empty.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void waitUntilEmpty() throws InterruptedException {
        queue.waitUntilEmpty();
    }

    /**
     * Closes this instance.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void close() throws IOException {
        queue.close();
    }

    /**
     * Deletes a file and all empty ancestor directories up to (but not
     * including) the root-directory. If the file doesn't exist, then that fact
     * is logged at the informational level.
     * 
     * @param path
     *            Pathname of the file to delete.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void delete(final Path path) throws IOException {
        try {
            Files.delete(path);
            deletedCount++;
        }
        catch (final NoSuchFileException e) {
            logger.info("Couldn't delete non-existant file \"{}\"", path);
        }
        for (Path dir = path.getParent(); dir != null
                && !Files.isSameFile(dir, rootDir); dir = dir.getParent()) {
            if (isEmpty(dir)) {
                try {
                    Files.delete(dir);
                }
                catch (final DirectoryNotEmptyException e) {
                    // A file must have just been added.
                    break;
                }
            }
        }
    }

    /**
     * Indicates if a directory is empty.
     * 
     * @param dir
     *            Pathname of the directory in question.
     * @return {@code true} if and only if the directory is empty.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static boolean isEmpty(final Path dir) throws IOException {
        final DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
        try {
            return !stream.iterator().hasNext();
        }
        finally {
            stream.close();
        }
    }
}
