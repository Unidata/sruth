/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;
import edu.ucar.unidata.dynaccn.MinHeapFile.Element;

/**
 * Priority queue of files to delete based on their deletion-time.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class FileDeletionQueue {
    /**
     * An entry in the min-heap file.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @NotThreadSafe
    private static class Entry extends MinHeapFile.Element {
        /**
         * Time when the associated file should be deleted in milliseconds since
         * the beginning of the epoch (1970-01-01 00:00:00 UTC).
         */
        private long   deletionTime;
        /**
         * Pathname of the associated file as an array of bytes.
         */
        private byte[] path;

        /**
         * Constructs from nothing.
         */
        @SuppressWarnings("unused")
        Entry() {
        }

        /**
         * Constructs from the pathname of the file and the time when it should
         * be deleted.
         * 
         * @param path
         *            Pathname of the file to be deleted.
         * @param deletionTime
         *            Time when the file should be deleted in milliseconds since
         *            1970-01-01 00:00:00 UTC.
         * @throws IllegalArgumentException
         *             if {@code path.toString().getBytes().length > }
         *             {@link FileDeletionQueue#MAX_PATH_LEN}
         * @throws NullPointerException
         *             if {@code path == null || deletionTime == null}.
         */
        Entry(final Path path, final Long deletionTime) {
            this(path.toString().getBytes(), deletionTime);
        }

        /**
         * Constructs from the pathname of the file as an array of bytes and the
         * time when it should be deleted in milliseconds since the epoch.
         * 
         * @param path
         *            The pathname of the file as a sequence of bytes.
         * @param deletionTime
         *            The time when the file should be deleted in milliseconds
         *            since the beginning of the epoch (1970-01-01 00:00:00
         *            UTC).
         * @throws IllegalArgumentException
         *             if {@code path.length > }
         *             {@link FileDeletionQueue#MAX_PATH_LEN}
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        private Entry(final byte[] path, final long deletionTime) {
            if (path.length > MAX_PATH_LEN) {
                throw new IllegalArgumentException("Pathname too long: \""
                        + new String(path) + "\"");
            }
            this.path = path;
            this.deletionTime = deletionTime;
        }

        /**
         * Returns the path attribute.
         * 
         * @return The path attribute.
         */
        Path getPath() {
            return Paths.get(new String(path));
        }

        /**
         * Returns the deletion-time attribute.
         * 
         * @return The deletion-time attribute.
         */
        long getDeletionTime() {
            return deletionTime;
        }

        @Override
        public void write(final ByteBuffer out) throws IOException {
            out.putLong(deletionTime);
            out.putShort((short) path.length);
            out.put(path);
        }

        @Override
        public void read(final ByteBuffer in) throws IOException {
            deletionTime = in.getLong();
            if (deletionTime < 0) {
                throw new InvalidObjectException("Invalid deletion time: "
                        + new Date(deletionTime));
            }
            final short len = in.getShort();
            if (len <= 0 || len > MAX_PATH_LEN) {
                throw new InvalidObjectException("Invalid pathname length: "
                        + len);
            }
            path = new byte[len];
            in.get(path);
        }

        @Override
        public int compareTo(final Element o) {
            final Entry that = (Entry) o;
            return deletionTime < that.deletionTime
                    ? -1
                    : deletionTime > that.deletionTime
                            ? 1
                            : compare(path, that.path);
        }

        /**
         * Compares two byte arrays.
         * 
         * @param ba1
         *            The first byte array.
         * @param ba2
         *            The second byte array.
         * @return A value less than, equal to, or greater than zero as the
         *         first argument is considered less than, equal to, or greater
         *         than the second, respectively.
         */
        private int compare(final byte[] ba1, final byte[] ba2) {
            final int minLen = ba1.length < ba2.length
                    ? ba1.length
                    : ba2.length;
            for (int i = 0; i < minLen; i++) {
                if (ba1[i] < ba2[i]) {
                    return -1;
                }
                if (ba1[i] > ba2[i]) {
                    return 1;
                }
            }
            return ba1.length < ba2.length
                    ? -1
                    : ba1.length == ba2.length
                            ? 0
                            : 1;
        }

        /**
         * Returns the maximum size of an entry in bytes.
         * 
         * @return The maximum size of an entry in bytes.
         */
        static int getMaxSize() {
            return 8 + 2 + MAX_PATH_LEN; // long + short + string
        }
    }

    /**
     * The maximum length of a pathname in bytes ({@value} ).
     */
    static final int                 MAX_PATH_LEN = 255;
    /**
     * The min-heap file that implements the priority queue.
     */
    @GuardedBy("this")
    private final MinHeapFile<Entry> heap;

    /**
     * Constructs from the pathname of the queue. If the queue exists, then it
     * will be opened; otherwise, it will be created.
     * 
     * @param path
     *            The pathname of the queue.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IOException
     *             if an I/O error occurs.
     */
    FileDeletionQueue(final Path path) throws IOException {
        heap = new MinHeapFile<Entry>(path, Entry.getMaxSize(), Entry.class);
    }

    /**
     * Adds an entry to the queue.
     * 
     * @param path
     *            The pathname of the file to be deleted.
     * @param time
     *            The time when the file should be deleted in milliseconds since
     *            1970-01-01 00:00:00 UTC.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalArgumentException
     *             if {@code path.toString().getBytes().length > }
     *             {@link #MAX_PATH_LEN}
     */
    synchronized void add(final Path path, final long time) throws IOException {
        try {
            heap.add(new Entry(path, time));
            notifyAll();
        }
        catch (final InstantiationException impossible) {
            throw new AssertionError(impossible);
        }
        catch (final IllegalAccessException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /**
     * Returns the number of entries in the queue.
     * 
     * @return The number of entries in the queue.
     */
    int size() {
        /*
         * Synchronization is unnecessary because the heap is thread-safe.
         */
        return heap.size();
    }

    /**
     * Retrieves (but does not remove) the pathname of the file that should be
     * deleted next. Blocks until the earliest file-deletion time has arrived.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized Path peek() throws InterruptedException, IOException {
        try {
            Entry entry;
            while ((entry = heap.peek()) == null) {
                wait();
            }
            long sleep;
            while ((sleep = heap.peek().getDeletionTime()
                    - System.currentTimeMillis()) > 0) {
                wait(sleep);
            }
            return entry.getPath();
        }
        catch (final InstantiationException impossible) {
            throw new AssertionError(impossible);
        }
        catch (final IllegalAccessException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /**
     * Removes the head of the queue and returns the associated pathname.
     * 
     * @return The pathname associated with the head of the queue or {@code
     *         null} if the queue is empty.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized Path remove() throws IOException {
        Entry entry;
        try {
            entry = (Entry) heap.remove();
            if (entry == null) {
                return null;
            }
            notifyAll();
            return entry.getPath();
        }
        catch (final InstantiationException impossible) {
            throw new AssertionError(impossible);
        }
        catch (final IllegalAccessException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /**
     * Waits until the queue is empty.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    synchronized void waitUntilEmpty() throws InterruptedException {
        while (size() > 0) {
            wait();
        }
    }

    /**
     * Closes this instance.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void close() throws IOException {
        heap.close();
    }
}
