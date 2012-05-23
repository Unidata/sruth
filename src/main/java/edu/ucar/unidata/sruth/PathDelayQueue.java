/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;

import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.MinHeapFile.Element;

/**
 * Priority queue of pathnames to become available based on their delay-times.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class PathDelayQueue {
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
         * Time when the associated pathname should become available in
         * milliseconds since the beginning of the epoch (1970-01-01 00:00:00
         * UTC).
         */
        private long   time;
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
         * become available.
         * 
         * @param path
         *            Pathname of the file to become available.
         * @param time
         *            Time when the file should be deleted in milliseconds since
         *            1970-01-01 00:00:00 UTC.
         * @throws IllegalArgumentException
         *             if {@code path.toString().getBytes().length > }
         *             {@link PathDelayQueue#MAX_PATH_LEN}
         * @throws NullPointerException
         *             if {@code path == null || time == null}.
         */
        Entry(final Path path, final Long time) {
            this(path.toString().getBytes(), time);
        }

        /**
         * Constructs from the pathname of the file as an array of bytes and the
         * time when it should become available in milliseconds since the epoch.
         * 
         * @param path
         *            The pathname of the file as a sequence of bytes.
         * @param time
         *            The time when the file should become available in
         *            milliseconds since the beginning of the epoch (1970-01-01
         *            00:00:00 UTC).
         * @throws IllegalArgumentException
         *             if {@code path.length > }
         *             {@link PathDelayQueue#MAX_PATH_LEN}
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        private Entry(final byte[] path, final long time) {
            if (path.length > MAX_PATH_LEN) {
                throw new IllegalArgumentException("Pathname too long: \""
                        + new String(path) + "\"");
            }
            this.path = path;
            this.time = time;
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
         * Returns the time attribute.
         * 
         * @return The time attribute.
         */
        long getTime() {
            return time;
        }

        @Override
        public void write(final ByteBuffer out) throws IOException {
            out.putLong(time);
            out.putShort((short) path.length);
            out.put(path);
        }

        @Override
        public void read(final ByteBuffer in) throws IOException {
            time = in.getLong();
            if (time < 0) {
                throw new InvalidObjectException("Invalid time: "
                        + new Date(time));
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
            return time < that.time
                    ? -1
                    : time > that.time
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

        @Override
        public String toString() {
            final DateFormat dateFormat = DateFormat.getInstance();
            final Calendar calendar = dateFormat.getCalendar();
            calendar.setTimeInMillis(time);
            final Date date = calendar.getTime();
            final String timestamp = dateFormat.format(date);
            return getClass().getSimpleName() + "[when=" + timestamp
                    + ", path=" + new String(path) + "]";
        }
    }

    /**
     * The logger for the package.
     */
    private static Logger            logger       = Util.getLogger();
    /**
     * The maximum length of a pathname in bytes ({@value} ).
     */
    static final int                 MAX_PATH_LEN = 255;
    /**
     * The min-heap file that implements the priority queue.
     */
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
    PathDelayQueue(final Path path) throws IOException {
        heap = new MinHeapFile<Entry>(path, Entry.getMaxSize(), Entry.class);
    }

    /**
     * Adds an entry to the queue.
     * 
     * @param path
     *            The pathname of the file to become available.
     * @param time
     *            The time when the file should become available in milliseconds
     *            since 1970-01-01 00:00:00 UTC.
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
        // Synchronization is unnecessary because "heap" is thread-safe
        return heap.size();
    }

    /**
     * Retrieves (but does not remove) the pathname of the file that should be
     * deleted next. Blocks until the earliest availability-time has arrived.
     * 
     * @return The next available pathname.
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
            while ((sleep = heap.peek().getTime() - System.currentTimeMillis()) > 0) {
                wait(sleep);
            }
            logger.trace("Returned {}", entry.toString());
            return entry.getPath();
        }
        catch (final InstantiationException impossible) {
            throw new AssertionError(impossible);
        }
        catch (final IllegalAccessException impossible) {
            throw new AssertionError(impossible);
        }
        catch (final ClosedByInterruptException e) {
            throw (InterruptedException) new InterruptedException()
                    .initCause(e);
        }
    }

    /**
     * Removes the head of the queue and returns the associated pathname.
     * 
     * @return The pathname associated with the head of the queue or
     *         {@code null} if the queue is empty.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized Path remove() throws IOException {
        Entry entry;
        try {
            entry = heap.remove();
            if (entry == null) {
                return null;
            }
            logger.trace("Removed {}", entry.toString());
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
    synchronized void close() throws IOException {
        heap.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PathDelayQueue [heap=" + heap + "]";
    }

    /**
     * Lists the contents of a pathname/delay queue.
     * <p>
     * Usage:
     * 
     * <pre>
     * edu.ucar.unidata.sruth.PathDelayQueue path
     *     
     * where:
     *   path           Pathname of the pathname/delay queue.
     * </pre>
     * <p>
     * Exit status:
     * 
     * <pre>
     *   0  Success: the contents of the file were listed.
     *   1  Invalid invocation
     * </pre>
     * 
     * @param args
     *            Program arguments
     * 
     * @throws IOException
     */
    @SuppressWarnings("unused")
    public static void main(final String[] args) throws IOException {
        final int INVALID_INVOCATION = 1;
        Path path = null;

        try {
            int iarg;
            String arg;
            /*
             * Process the optional arguments.
             */
            for (iarg = 0; iarg < args.length; ++iarg) {
                arg = args[iarg];
                try {
                    if (arg.charAt(0) != '-') {
                        break;
                    }
                    final String optString = arg.substring(1);
                    arg = args[++iarg];
                    {
                        logger.error("Invalid option: \"{}\"", optString);
                        throw new IllegalArgumentException();
                    }
                }
                catch (final IndexOutOfBoundsException e) {
                    logger.error("Invalid argument: \"{}\"", arg);
                    throw new IllegalArgumentException();
                }
            }

            /*
             * Process the subscription argument.
             */
            if (iarg >= args.length) {
                logger.error("The path argument is missing");
                throw new IllegalArgumentException();
            }
            arg = args[iarg++];
            path = Paths.get(arg);

            if (iarg < args.length) {
                logger.error("Too many arguments");
                throw new IllegalArgumentException();
            }
        }
        catch (final IllegalArgumentException e) {
            logger.error("Usage: " + PathDelayQueue.class.getCanonicalName()
                    + " pathname");
            System.exit(INVALID_INVOCATION);
        }

        final PathDelayQueue queue = new PathDelayQueue(path);

        final DateFormat dateFormat = DateFormat.getInstance();
        final Calendar calendar = dateFormat.getCalendar();

        for (final Entry entry : queue.heap) {
            System.out.println(entry.toString());
        }
    }
}
