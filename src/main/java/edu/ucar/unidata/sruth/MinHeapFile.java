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
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

/**
 * A file that implements a persistent minimum-heap (i.e., elements are stored
 * in order of increasing value).
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class MinHeapFile<E extends MinHeapFile.Element> implements Iterable<E> {
    /**
     * The I/O handler for the heap-file.
     * <p>
     * This class is thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static class IoHandler {
        /**
         * The buffer used by this class.
         * <p>
         * This class is thread-safe.
         * 
         * @author Steven R. Emmerson
         */
        private class MyBuffer {
            /**
             * The underlying byte-buffer
             */
            private final ByteBuffer buf;

            /**
             * Constructs from a memory-mapped byte-buffer.
             * 
             * @param buf
             *            The memory-mapped byte-buffer
             */
            MyBuffer(final ByteBuffer buf) {
                this.buf = buf;
            }

            /**
             * Writes an integer into the buffer.
             * 
             * @param value
             */
            void putInt(final int value) {
                buf.putInt(value);
            }

            /**
             * Returns the current position of the buffer.
             * 
             * @return The current position of the buffer.
             */
            int position() {
                return buf.position();
            }

            /**
             * Saves the contents of the buffer.
             */
            void force() {
                IoHandler.this.force();
            }

            /**
             * Returns the integer at the current buffer position.
             * 
             * @return the integer at the current buffer position.
             */
            int getInt() {
                return buf.getInt();
            }

            /**
             * Rewinds the buffer (i.e., sets the position to 0).
             */
            void rewind() {
                buf.rewind();
            }

            /**
             * Returns the byte-buffer that backs this instance.
             * 
             * @return the byte-buffer that backs this instance.
             */
            ByteBuffer getByteBuffer() {
                return buf;
            }
        }

        /**
         * The I/O channel to the file.
         */
        private final FileChannel channel;
        /**
         * The memory-mapped byte-buffer that is the entire file.
         */
        @GuardedBy("this")
        private MappedByteBuffer  buf;

        /**
         * Constructs from the pathname of the file.
         * 
         * @param path
         *            The pathname of the file.
         * @throws IOException
         *             if an I/O error occurs
         */
        IoHandler(final Path path) throws IOException {
            channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            initMappedBuffer();
        }

        /**
         * Initializes the memory-mapped byte-buffer.
         * 
         * @throws IOException
         *             if an I/O error occurs
         */
        private synchronized void initMappedBuffer() throws IOException {
            buf = channel.map(MapMode.READ_WRITE, 0, channel.size());
        }

        /**
         * Forces any changes to the memory-mapped byte-buffer to the underlying
         * file.
         */
        private synchronized void force() {
            buf.force();
        }

        /**
         * Returns the size of the file in bytes.
         * 
         * @return the size of the file in bytes.
         * @throws IOException
         *             if an I/O error occurs
         */
        int size() throws IOException {
            return (int) channel.size();
        }

        /**
         * Returns a buffer corresponding to an existing region of the file.
         * 
         * @param position
         *            The offset, in bytes, to the start of the region
         * @param length
         *            The length of the region in bytes
         * @return a byte-buffer corresponding to the specified region
         * @throws IOException
         *             if an I/O error occurs
         */
        synchronized MyBuffer getBuffer(final int position, final int length)
                throws IOException {
            buf.position(position);
            final ByteBuffer regBuf = buf.slice();
            regBuf.limit(length);
            return new MyBuffer(regBuf);
        }

        /**
         * Ensures that the underlying file is at least a given size.
         * 
         * @param size
         *            The size of the file in bytes
         * @throws IOException
         *             if an I/O error occurs
         * @throws IllegalArgumentException
         *             if {@code size < 0}
         */
        synchronized void ensureSize(final long size) throws IOException {
            if (size < 0) {
                throw new IllegalArgumentException("Invalid size: " + size);
            }
            if (channel.size() < size) {
                channel.position(size - 1);
                channel.write(ByteBuffer.wrap(new byte[] { 0 }));
                channel.force(false);
                initMappedBuffer();
            }
        }

        /**
         * Locks a region of the underlying file.
         * 
         * @param position
         *            The offset, in bytes, to the start of the region.
         * @param size
         *            The length of the region in bytes
         * @param shared
         *            Whether or not the lock is sharable.
         * @return a lock on the region.
         * @throws IOException
         *             if an I/O error occurs
         */
        synchronized FileLock lock(final long position, final int size,
                final boolean shared) throws IOException {
            return channel.lock(position, size, shared);
        }

        /**
         * Forces any changes to a byte-buffer to the underlying file.
         * 
         * @param buffer
         *            The byte-buffer
         */
        void force(final ByteBuffer buffer) {
            /*
             * Bit of a hack. Only necessary because ByteBuffer can't be
             * extended.
             */
            force();
        }

        /**
         * Closes this instance, releasing all resources. Idempotent.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        synchronized void close() throws IOException {
            if (channel.isOpen()) {
                channel.close();
            }
        }
    }

    /**
     * The header of the heap-file.
     * <p>
     * This class is thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private static class Header {
        /**
         * The version of this class.
         */
        private static final short       VERSION     = 1;
        /**
         * The size of the file-header. The file-header comprise the following
         * parameters in order: version (4 bytes), eltSize (4 bytes), and
         * eltCount (4 bytes).
         */
        private static final int         HEADER_SIZE = 12;
        /**
         * The buffer for the number of elements.
         */
        private final IoHandler.MyBuffer eltCountBuffer;
        /**
         * The size of an element.
         */
        private final int                eltSize;
        /**
         * The number of elements in the heap-file.
         */
        @GuardedBy("this")
        private int                      eltCount;

        /**
         * Constructs from an I/O handler, the starting offset, and the size of
         * an element.
         * 
         * @param channel
         *            The channel to the file.
         * @param position
         *            The offset into the file, in bytes, for the start of the
         *            header.
         * @param eltSize
         *            The size of an element in bytes.
         * @throws IOException
         *             if an I/O error occurs
         */
        Header(final IoHandler ioHandler, final int position, final int eltSize)
                throws IOException {
            int eltCountPosition;
            if (position + HEADER_SIZE > ioHandler.size()) {
                /*
                 * The header doesn't exist. Create it.
                 */
                ioHandler.ensureSize(position + HEADER_SIZE);
                final IoHandler.MyBuffer headBuf = ioHandler.getBuffer(
                        position, HEADER_SIZE);
                headBuf.putInt(VERSION);
                headBuf.putInt(eltSize);
                eltCountPosition = headBuf.position();
                headBuf.putInt(0);
                headBuf.force();
            }
            else {
                /*
                 * The header exists. Vet it.
                 */
                final IoHandler.MyBuffer headBuf = ioHandler.getBuffer(
                        position, HEADER_SIZE);
                final int version = headBuf.getInt();
                if (version != VERSION) {
                    throw new IllegalStateException(
                            "Corrupt file: unexpected version: " + version
                                    + " != " + VERSION);
                }
                final int actualEltSize = headBuf.getInt();
                if (actualEltSize != eltSize) {
                    throw new IllegalArgumentException(
                            "Element size inconsistancy: " + actualEltSize
                                    + " != " + eltSize);
                }
                eltCountPosition = headBuf.position();
                eltCount = headBuf.getInt();
                if (eltCount < 0) {
                    throw new IllegalStateException(
                            "Corrupt file: negative element count: " + eltCount);
                }
            }
            eltCountBuffer = ioHandler.getBuffer(eltCountPosition, Integer.SIZE
                    / Byte.SIZE);
            this.eltSize = eltSize;
        }

        /**
         * Returns the size of the header.
         * 
         * @return The size of the header in bytes.
         */
        int getSize() {
            return HEADER_SIZE;
        }

        /**
         * Returns the size of an element.
         * 
         * @return The size of an element.
         */
        int getEltSize() {
            return eltSize;
        }

        /**
         * Sets the number of elements in the file.
         */
        synchronized void setEltCount(final int eltCount) {
            this.eltCount = eltCount;
            eltCountBuffer.rewind();
            eltCountBuffer.putInt(eltCount);
            eltCountBuffer.force();
        }

        /**
         * Returns the number of elements in the file.
         * 
         * @return The number of elements in the file.
         */
        synchronized int getEltCount() {
            return eltCount;
        }
    }

    /**
     * The portion of the file that contains all the elements.
     * <p>
     * This class is thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private static class Heap<E extends MinHeapFile.Element> {
        /**
         * The I/O handler for the underlying file.
         */
        private final IoHandler ioHandler;
        /**
         * The file-header.
         */
        private final Header    header;
        /**
         * The offset, in bytes, to the start of the elements in the file.
         */
        private final int       position;
        /**
         * The type of an element.
         */
        private final Class<E>  type;

        /**
         * Constructs from an I/O handler, the file-header, the offset to the
         * start of the elements, and the type of an element.
         * 
         * @param ioHandler
         *            The I/O handler for the file.
         * @param header
         *            The file-header.
         * @param position
         *            The offset, in bytes, to the start of the elements in the
         *            file.
         * @param type
         *            The type of an element.
         * @throws IOException
         *             if an I/O error occurs
         */
        Heap(final IoHandler ioHandler, final Header header,
                final int position, final Class<E> type) throws IOException {
            this.ioHandler = ioHandler;
            this.header = header;
            this.position = position;
            this.type = type;
        }

        /**
         * Returns an existing element.
         * 
         * @param index
         *            Index of the existing element to return.
         * @return The element at the corresponding index
         * @throws IllegalAccessException
         *             if an element can't be created.
         * @throws InstantiationException
         *             if an element can't be created.
         * @throws ClosedChannelException
         *             if the channel to the file is closed
         * @throws IOException
         *             if an I/O error occurs.
         */
        E getElt(final int index) throws ClosedChannelException,
                InstantiationException, IllegalAccessException, IOException {
            final FileLock lock = ioHandler.lock(offsetToElement(index),
                    header.getEltSize(), true);
            try {
                final E instance = type.newInstance();
                instance.read(getEltBuffer(index));
                return instance;
            }
            finally {
                lock.release();
            }
        }

        /**
         * Sets an existing element to a given element.
         * 
         * @param index
         *            Index of the existing element to be set.
         * @param elt
         *            The given element.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void setElt(final int index, final E elt) throws IOException {
            final FileLock lock = ioHandler.lock(offsetToElement(index),
                    header.getEltSize(), false);
            try {
                final ByteBuffer buffer = getEltBuffer(index);
                elt.write(buffer);
                ioHandler.force(buffer);
            }
            finally {
                lock.release();
            }
        }

        /**
         * Returns the absolute offset, in bytes, to the start of an element.
         * 
         * @param index
         *            The index of the element
         * @return the absolute offset, in bytes, to the start of the element.
         * @throws IllegalArgumentException
         *             if {@code index < 0}
         */
        private int offsetToElement(final int index) {
            if (index < 0) {
                throw new IllegalArgumentException("Invalid index: " + index);
            }
            return position + index * header.getEltSize();
        }

        /**
         * Returns an I/O byte-buffer for an existing element.
         * 
         * @param index
         *            The index of the existing element.
         * @return An I/O byte-buffer for the specified element.
         * @throws ClosedChannelException
         *             if the channel to the file is closed
         * @throws IllegalArgumentException
         *             if {@code index} is too large.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private ByteBuffer getEltBuffer(final int index)
                throws ClosedChannelException, IOException {
            final IoHandler.MyBuffer buf = ioHandler.getBuffer(
                    offsetToElement(index), header.getEltSize());
            return buf.getByteBuffer();
        }
    }

    /**
     * The element of the heap-file.
     * <p>
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
     * The type of an element.
     */
    private final Class<E>  type;
    /**
     * The header of the heap-file.
     */
    private final Header    header;
    /**
     * The I/O handler for the file.
     */
    private final IoHandler ioHandler;
    /**
     * The heap of elements.
     */
    private final Heap<E>   elements;
    /**
     * The capacity of this instance in elements.
     */
    @GuardedBy("this")
    private int             capacity;

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
    MinHeapFile(final Path path, final int eltSize, final Class<E> type)
            throws IOException {
        if (eltSize <= 0) {
            throw new IllegalArgumentException(
                    "Non-positive maximum element size: " + eltSize);
        }
        if (type == null) {
            throw new NullPointerException();
        }
        this.type = type;
        ioHandler = new IoHandler(path);
        header = new Header(ioHandler, 0, eltSize);
        elements = new Heap<E>(ioHandler, header, header.getSize(), type);
        synchronized (this) {
            capacity = (ioHandler.size() - header.getSize())
                    / header.getEltSize();
        }
    }

    /**
     * Adds an element. Does so in a way that a power failure might cause the
     * heap to contain an extra element.
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
    synchronized void add(final E elt) throws IOException,
            InstantiationException, IllegalAccessException {
        int childIndex;
        int parentIndex;
        final int eltCount = header.getEltCount();
        ensureCapacity(header.getEltCount() + 1);
        for (childIndex = eltCount; childIndex > 0; childIndex = parentIndex) {
            parentIndex = (childIndex - 1) / 2;
            final E parent = elements.getElt(parentIndex);
            if (parent.compareTo(elt) <= 0) {
                break;
            }
            elements.setElt(childIndex, parent);
        }
        elements.setElt(childIndex, elt);
        header.setEltCount(eltCount + 1);
    }

    /**
     * Ensures that this instance can contain the given number of elements.
     * 
     * @param eltCount
     *            The given number of elements.
     * @throws IOException
     *             if an I/O error occurs
     * @throws IllegalArgumentException
     *             if {@code eltCount < 0}
     */
    private synchronized void ensureCapacity(final int eltCount)
            throws IOException {
        if (eltCount < 0) {
            throw new IllegalArgumentException("Invalid capacity: " + eltCount);
        }
        if (eltCount > capacity) {
            final int newCapacity = (int) Math.round(eltCount * 1.61803399);
            ioHandler.ensureSize(header.getSize() + newCapacity
                    * header.getEltSize());
            capacity = newCapacity;
        }
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
    synchronized E peek() throws ClosedByInterruptException,
            InstantiationException, IllegalAccessException, IOException {
        return (header.getEltCount() > 0)
                ? elements.getElt(0)
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
    synchronized E remove() throws ClosedChannelException, IOException,
            InstantiationException, IllegalAccessException {
        E firstElt;
        int eltCount = header.getEltCount();
        if (eltCount <= 0) {
            firstElt = null;
        }
        else {
            firstElt = elements.getElt(0);
            final E elt = elements.getElt(--eltCount);
            int parentIndex = 0;
            for (int childIndex = 1; childIndex < eltCount; childIndex = 2 * parentIndex + 1) {
                E child = elements.getElt(childIndex);
                if ((childIndex + 1 < eltCount)) {
                    final E otherChild = elements.getElt(childIndex + 1);
                    if (child.compareTo(otherChild) > 0) {
                        child = otherChild;
                        childIndex++;
                    }
                }
                if (child.compareTo(elt) >= 0) {
                    break;
                }
                elements.setElt(parentIndex, child);
                parentIndex = childIndex;
            }
            elements.setElt(parentIndex, elt);
            header.setEltCount(eltCount);
        }
        return firstElt;
    }

    /**
     * Returns the number of elements in this instance.
     * 
     * @return The number of elements in this instance.
     */
    int size() {
        return header.getEltCount();
    }

    /**
     * Closes this instance. After this method, calling any method that attempts
     * to access the heap will cause an exception to be thrown.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void close() throws IOException {
        ioHandler.close();
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                final MinHeapFile<E> heap = MinHeapFile.this;
                synchronized (heap) {
                    return index < header.getEltCount();
                }
            }

            @Override
            public E next() {
                try {
                    return elements.getElt(index++);
                }
                catch (final Exception e) {
                    throw (NoSuchElementException) new NoSuchElementException(
                            "Index=" + index).initCause(e);
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
    public synchronized String toString() {
        return "MinHeapFile [eltCount=" + size() + ", type=" + type
                + ", eltSize=" + header.getEltSize() + "]";
    }
}
