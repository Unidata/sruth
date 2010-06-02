/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A data-specification comprising piece-specifications for multiple files.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class MultiFilePieceSpecs implements PieceSpecSet, Serializable {
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
    MultiFilePieceSpecs() {
    }

    /**
     * Constructs from a set of piece-specifications for a single file.
     * 
     * @param specs
     *            The set of piece-specifications for a single file.
     */
    MultiFilePieceSpecs(final FilePieceSpecSet specs) {
        synchronized (this) {
            add(specs);
        }
    }

    @Override
    public PieceSpecSet merge(final PieceSpecSet specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSet merge(final MultiFilePieceSpecs that) {
        MultiFilePieceSpecs o1, o2;
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
                    final MultiFilePieceSpecs tmp = o1;
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
    public PieceSpecSet merge(final FilePieceSpecs specs) {
        add(specs);
        return this;
    }

    @Override
    public synchronized PieceSpecSet merge(final PieceSpec spec) {
        add(spec);
        return this;
    }

    /**
     * Adds a set of piece-specifications for a file to this instance.
     * 
     * @param specs
     *            The set of piece-specifications for a file.
     */
    @GuardedBy("this")
    private void add(final FilePieceSpecSet specs) {
        final FileId fileId = specs.getFileId();
        FilePieceSpecSet entry = filePieceSpecSets.remove(fileId);
        entry = (FilePieceSpecSet) ((null == entry)
                ? specs
                : entry.merge(specs));
        filePieceSpecSets.put(fileId, entry);
    }

    @Override
    public boolean isEmpty() {
        return filePieceSpecSets.isEmpty();
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
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("MultiFilePieceSpecs [filePieceSpecSets=");
        builder.append(filePieceSpecSets);
        builder.append("]");
        return builder.toString();
    }

    private Object readResolve() {
        /*
         * For all map entries, ensure that the key's file-identifier equals the
         * value's file-identifier.
         */
        final MultiFilePieceSpecs result = new MultiFilePieceSpecs();
        for (final FilePieceSpecSet specs : filePieceSpecSets.values()) {
            result.add(specs);
        }
        return result;
    }
}
