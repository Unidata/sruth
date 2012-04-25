/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;

/**
 * A file that implements a persistent minimum-heap (i.e., elements are stored
 * in order of increasing value).
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class MinHeapFile<T extends MinHeapFile.Element> implements Iterable<T> {
    /**
     * The element of the heap-file.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    @Immutable
    abstract static class Element implements Comparable<Element> {
        /**
         * Writes an entry to a byte-buffer.
         * 
         * @param out
         *            The output byte-buffer.
         * @throws IOException
         *             if an I/O error occurs.
         */
        public abstract void write(ByteBuffer out) throws IOException;

        /**
         * Reads an entry from a byte-buffer.
         * 
         * @param in
         *            The input byte-buffer.
         * @throws IOException
         *             if an I/O error occurs.
         */
        public abstract void read(ByteBuffer in) throws IOException;
    }

    /**
     * The version of this class.
     */
    private static final short     VERSION     = 1;
    /**
     * The size of the file-header. The file-header comprise the following
     * parameters in order: version (4 bytes), eltSize (4 bytes), and eltCount
     * (4 bytes).
     */
    private static final int       HEADER_SIZE = 12;
    /**
     * The I/O channel to the file.
     */
    private final FileChannel      channel;
    /**
     * The number of elements in the heap.
     */
    private int                    eltCount;
    /**
     * The type of an element.
     */
    private final Class<T>         type;
    /**
     * The I/O byte-buffer for the number of elements.
     */
    private final MappedByteBuffer eltCountBuffer;
    /**
     * The I/O byte-buffer for the elements.
     */
    private MappedByteBuffer       eltsBuffer;
    /**
     * The size of an element.
     */
    private final int              eltSize;

    /**
     * Constructs from the pathname of the file. The file is created if it
     * doesn't exist.
     * 
     * @param path
     *            The pathname of the file.
     * @param eltSize
     *            The size, in bytes, of an element in the heap-file.
     * @param type
     *            The element class. Must have an accessible, nullary
     *            constructor.
     * @throws IllegalArgumentException
     *             if the file exists but was created with a different
     *             {@code eltSize}.
     * @throws IllegalArgumentException
     *             if {@code eltSize <= 0}.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code type == null}.
     */
    MinHeapFile(final Path path, final int eltSize, final Class<T> type)
            throws IOException {
        if (eltSize <= 0) {
            throw new IllegalArgumentException(
                    "Non-positive maximum element size: " + eltSize);
        }
        if (type == null) {
            throw new NullPointerException();
        }
        this.type = type;
        this.eltSize = eltSize;
        channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        /*
         * The file-header comprise the following parameters in order: version
         * (4 bytes), eltSize (4 bytes), and eltCount (4 bytes).
         */
        if (channel.size() < HEADER_SIZE) {
            /*
             * The header doesn't exist. Create it.
             */
            ensureFileSize(HEADER_SIZE);
            final MappedByteBuffer headBuf = channel.map(MapMode.READ_WRITE, 0,
                    HEADER_SIZE);
            headBuf.putInt(VERSION);
            headBuf.putInt(eltSize);
            eltCountBuffer = channel.map(MapMode.READ_WRITE,
                    headBuf.position(), 4);
            eltCount = 0;
            writeEltCount();
            headBuf.force();
        }
        else {
            /*
             * The header exists. Vet it.
             */
            final ByteBuffer headBuf = channel.map(MapMode.READ_WRITE, 0,
                    HEADER_SIZE);
            final int version = headBuf.getInt();
            if (version != VERSION) {
                throw new IllegalStateException(
                        "Corrupt file: unexpected version: " + version + " != "
                                + VERSION);
            }
            final int actualEltSize = headBuf.getInt();
            if (actualEltSize != eltSize) {
                throw new IllegalArgumentException(
                        "Element size inconsistancy: " + actualEltSize + " != "
                                + eltSize);
            }
            eltCountBuffer = channel.map(MapMode.READ_WRITE,
                    headBuf.position(), 4);
            eltCount = eltCountBuffer.getInt();
            if (eltCount < 0) {
                throw new IllegalStateException(
                        "Corrupt file: negative element count: " + eltCount);
            }
            if (channel.size() < HEADER_SIZE + getBufferOffset(eltCount)) {
                throw new IOException("Corrupt file: file too small: "
                        + channel.size() + " < " + HEADER_SIZE
                        + getBufferOffset(eltCount));
            }
        }
        this.eltsBuffer = getEltsBuffer();
    }

    /**
     * Adds an element. Does so in a way that a power failure might cause the
     * heap to contain a duplicate element, but no element will be lost.
     * 
     * @param elt
     *            The element to be added.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalAccessException
     *             if an I/O error occurs.
     * @throws InstantiationException
     *             if the new element can't be created
     */
    synchronized void add(final T elt) throws IOException,
            InstantiationException, IllegalAccessException {
        int childIndex;
        int parentIndex;
        for (childIndex = eltCount; childIndex > 0; childIndex = parentIndex) {
            parentIndex = (childIndex - 1) / 2;
            final T parent = readElt(parentIndex);
            if (parent.compareTo(elt) <= 0) {
                break;
            }
            writeElt(childIndex, parent);
        }
        ++eltCount;
        writeEltCount();
        writeElt(childIndex, elt);
    }

    /**
     * Returns (but does not remove) the first element in the heap.
     * 
     * @return the first element in the heap or {@code null} if the heap is
     *         empty.
     * @throws ClosedByInterruptException
     *             if the current thread was interrupted
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    synchronized T peek() throws ClosedByInterruptException,
            InstantiationException, IllegalAccessException, IOException {
        return (eltCount > 0)
                ? readElt(0)
                : null;
    }

    /**
     * Removes and returns the first element in the heap. Does so in a way that
     * a power failure might cause the heap to contain a duplicate element, but
     * no element will be lost.
     * 
     * @return The first element in the heap or {@code null} if the heap is
     *         empty.
     * @throws ClosedChannelException
     *             if the channel to the file is closed
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalAccessException
     *             if an element can't be created.
     * @throws InstantiationException
     *             if an element can't be created.
     */
    synchronized T remove() throws ClosedChannelException, IOException,
            InstantiationException, IllegalAccessException {
        T firstElt;
        if (eltCount <= 0) {
            firstElt = null;
        }
        else {
            firstElt = readElt(0);
            final T elt = readElt(--eltCount);
            int parentIndex = 0;
            for (int childIndex = 1; childIndex < eltCount; childIndex = 2 * parentIndex + 1) {
                T child = readElt(childIndex);
                if ((childIndex + 1 < eltCount)) {
                    final T otherChild = readElt(childIndex + 1);
                    if (child.compareTo(otherChild) > 0) {
                        child = otherChild;
                        childIndex++;
                    }
                }
                if (child.compareTo(elt) >= 0) {
                    break;
                }
                writeElt(parentIndex, child);
                parentIndex = childIndex;
            }
            writeElt(parentIndex, elt);
            writeEltCount();
        }
        return firstElt;
    }

    /**
     * Returns the number of elements in this instance.
     * 
     * @return The number of elements in this instance.
     */
    synchronized int size() {
        return eltCount;
    }

    /**
     * Closes this instance. After this method, calling any method that attempts
     * to access the heap will cause an exception to be thrown.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void close() throws IOException {
        channel.close();
    }

    /**
     * Writes the number of elements to the file.
     */
    private void writeEltCount() {
        eltCountBuffer.rewind();
        eltCountBuffer.putInt(eltCount);
        eltCountBuffer.force();
    }

    /**
     * Writes an element.
     * 
     * @param index
     *            Index of the element to be written.
     * @param elt
     *            Value of the element.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void writeElt(final int index, final T elt) throws IOException {
        final ByteBuffer buffer = getEltBuffer(index);
        elt.write(buffer);
        eltsBuffer.force();
    }

    /**
     * Returns an element.
     * 
     * @param index
     *            Index of the element to return.
     * @return The element at the corresponding position
     * @throws IllegalAccessException
     *             if an element can't be created.
     * @throws InstantiationException
     *             if an element can't be created.
     * @throws ClosedChannelException
     *             if the channel to the file is closed
     * @throws IOException
     *             if an I/O error occurs.
     */
    private T readElt(final int index) throws ClosedChannelException,
            InstantiationException, IllegalAccessException, IOException {
        final T instance = type.newInstance();
        instance.read(getEltBuffer(index));
        return instance;
    }

    /**
     * Returns an I/O byte-buffer for a given element.
     * 
     * @param index
     *            The index of the element.
     * @return An I/O bite-buffer for the specified element.
     * @throws ClosedChannelException
     *             if the channel to the file is closed
     * @throws IllegalArgumentException
     *             if {@code index} is too large.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private ByteBuffer getEltBuffer(final int index)
            throws ClosedChannelException, IOException {
        if (getMaxEltCount() < (index + 1)) {
            // The golden ratio is a space/time compromise
            final long newMaxEltCount = Math.round((index + 1) * 1.618034);
            if (newMaxEltCount > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Index too large: " + index);
            }
            final int maxEltCount = (int) newMaxEltCount;
            ensureFileSize(HEADER_SIZE + getBufferOffset(maxEltCount));
            eltsBuffer = getEltsBuffer();
        }
        eltsBuffer.position(getBufferOffset(index));
        final ByteBuffer eltBuffer = eltsBuffer.slice();
        eltBuffer.limit(eltSize);
        return eltBuffer;
    }

    /**
     * Returns a mapped byte buffer for the element portion of the I/O channel.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    private MappedByteBuffer getEltsBuffer() throws IOException {
        return channel.map(MapMode.READ_WRITE, HEADER_SIZE, channel.size()
                - HEADER_SIZE);
    }

    /**
     * Returns the position in the element buffer of the start of a given
     * element.
     * 
     * @param index
     *            the index of the element.
     * @return The position in the element buffer of the start of the element.
     */
    private int getBufferOffset(final int index) {
        return index * eltSize;
    }

    /**
     * Returns the maximum number of elements that the file can contain.
     * 
     * @return The maximum number of elements that the file can contain.
     * @throws ClosedChannelException
     *             if the channel to the file is closed
     * @throws IOException
     *             if an I/O error occurs.
     */
    private long getMaxEltCount() throws ClosedChannelException, IOException {
        return (channel.size() - HEADER_SIZE) / eltSize;
    }

    /**
     * Ensures that the size of the file is at least a given value.
     * 
     * @param size
     *            The minimum size of the file in bytes.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void ensureFileSize(final int size) throws IOException {
        if (channel.size() < size) {
            channel.position(size - 1);
            channel.write(ByteBuffer.wrap(new byte[] { 0 }));
        }
        // channel.force(false);
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                final MinHeapFile<T> heap = MinHeapFile.this;
                synchronized (heap) {
                    return index < heap.eltCount;
                }
            }

            @Override
            public T next() {
                final MinHeapFile<T> heap = MinHeapFile.this;
                synchronized (heap) {
                    try {
                        return heap.readElt(index++);
                    }
                    catch (final Exception e) {
                        throw (NoSuchElementException) new NoSuchElementException(
                                "Index=" + index).initCause(e);
                    }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "MinHeapFile [eltCount=" + eltCount + ", type=" + type
                + ", eltSize=" + eltSize + "]";
    }
}
