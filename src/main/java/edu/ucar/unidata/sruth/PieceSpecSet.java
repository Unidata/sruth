/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A set of piece-specifications.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class PieceSpecSet implements PieceSpecSetIface, Serializable {
    /**
     * The serial version ID.
     */
    private static final long                   serialVersionUID  = 1L;
    /**
     * The map from file identifier to the file's piece-specifications.
     * 
     * @serial The map from file identifier to the file's piece-specifications.
     */
    @GuardedBy("this")
    private final Map<FileId, FilePieceSpecSet> filePieceSpecSets = new TreeMap<FileId, FilePieceSpecSet>();

    /**
     * Constructs from nothing.
     */
    PieceSpecSet() {
    }

    /**
     * Constructs from a set of piece-specifications for a single file.
     * 
     * @param specs
     *            The set of piece-specifications for a single file.
     */
    PieceSpecSet(final FilePieceSpecSet specs) {
        add(specs);
    }

    @Override
    public PieceSpecSetIface merge(final PieceSpecSetIface specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSetIface merge(final PieceSpecSet that) {
        if (this == that) {
            return this;
        }
        PieceSpecSet o1, o2;
        if (System.identityHashCode(this) < System.identityHashCode(that)) {
            o1 = this;
            o2 = that;
        }
        else {
            o1 = that;
            o2 = this;
        }
        synchronized (o1) {
            synchronized (o2) {
                if (o1.filePieceSpecSets.size() < o2.filePieceSpecSets.size()) {
                    final PieceSpecSet tmp = o1;
                    o1 = o2;
                    o2 = tmp;
                }
                // "o2" has fewer entries
                for (final FilePieceSpecSet specs : o2.filePieceSpecSets
                        .values()) {
                    o1.add(specs);
                }
            }
        }
        return o1;
    }

    @Override
    public PieceSpecSetIface merge(final FilePieceSpecs specs) {
        add(specs);
        return this;
    }

    @Override
    public synchronized PieceSpecSetIface merge(final PieceSpec spec) {
        add(spec);
        return this;
    }

    @Override
    public synchronized PieceSpecSetIface remove(final PieceSpec spec) {
        final FileId fileId = spec.getFileId();
        final FilePieceSpecSet value = filePieceSpecSets.get(fileId);
        if (value != null) {
            final PieceSpecSetIface newValue = value.remove(spec);
            if (newValue != value) {
                if (newValue.isEmpty()) {
                    filePieceSpecSets.remove(fileId);
                }
                else {
                    filePieceSpecSets.put(fileId, (FilePieceSpecSet) newValue);
                }
            }
        }
        return this;
    }

    @Override
    public boolean contains(final PieceSpec spec) {
        final FileId fileId = spec.getFileId();
        final FilePieceSpecSet value = filePieceSpecSets.get(fileId);
        return value != null && value.contains(spec);
    }

    /**
     * Adds a set of piece-specifications for a file to this instance.
     * 
     * @param specs
     *            The set of piece-specifications for a file.
     */
    @GuardedBy("this")
    private synchronized void add(final FilePieceSpecSet specs) {
        final FileId fileId = specs.getFileId();
        FilePieceSpecSet value = filePieceSpecSets.get(fileId);
        value = (FilePieceSpecSet) ((null == value)
                ? specs
                : value.merge(specs));
        filePieceSpecSets.put(fileId, value);
    }

    @Override
    public synchronized boolean isEmpty() {
        return filePieceSpecSets.isEmpty();
    }

    @Override
    public synchronized PieceSpecSet clone() {
        /*
         * A constructor is used to create the clone because {@link
         * #filePieceSpecSets} is final (which means "super.clone()" will merely
         * copy the field; consequently, it would have to be non-final and
         * guarded, otherwise) and this class is final (which means no subclass
         * will call "super.clone()".
         */
        final PieceSpecSet clone = new PieceSpecSet();
        for (final FilePieceSpecSet specs : filePieceSpecSets.values()) {
            clone.add(specs.clone());
        }
        return clone;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public synchronized Iterator<PieceSpec> iterator() {
        return new SimpleIterator<PieceSpec>() {
            Iterator<FilePieceSpecSet> fileIterator;
            Iterator<PieceSpec>        specIterator;

            @Override
            protected PieceSpec getNext() {
                /*
                 * The fields are initialized here because
                 * declaration-initialization would occur only after this method
                 * is called by the super-constructor.
                 */
                if (null == fileIterator) {
                    fileIterator = filePieceSpecSets.values().iterator();
                    specIterator = fileIterator.hasNext()
                            ? fileIterator.next().iterator()
                            : null;

                }
                for (;;) {
                    if (null == specIterator) {
                        return null;
                    }
                    if (specIterator.hasNext()) {
                        return specIterator.next();
                    }
                    specIterator = fileIterator.hasNext()
                            ? fileIterator.next().iterator()
                            : null;
                }
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
        final StringBuilder builder = new StringBuilder();
        builder.append("PieceSpecSet [filePieceSpecSets=");
        final int size = filePieceSpecSets.size();
        if (size == 1) {
            builder.append(filePieceSpecSets);
        }
        else {
            builder.append("(");
            builder.append(size);
            builder.append(" entries)");
        }
        builder.append("]");
        return builder.toString();
    }

    private Object readResolve() {
        /*
         * For all map entries, ensure that the key's file-identifier equals the
         * value's file-identifier.
         */
        final PieceSpecSet result = new PieceSpecSet();
        for (final FilePieceSpecSet specs : filePieceSpecSets.values()) {
            result.add(specs);
        }
        return result;
    }
}
