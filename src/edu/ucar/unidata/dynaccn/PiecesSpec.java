package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;

/**
 * A data-specification comprising pieces of data in a file.
 * 
 * @author Steven R. Emmerson
 */
abstract class PiecesSpec implements Serializable {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the file.
     */
    protected final FileInfo  fileInfo;

    /**
     * Constructs from information on a file.
     * 
     * @param fileInfo
     *            Information on the file.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    protected PiecesSpec(final FileInfo fileInfo) {
        if (null == fileInfo) {
            throw new NullPointerException();
        }
        this.fileInfo = fileInfo;
    }

    /**
     * Returns a new instance.
     * 
     * @param fileInfo
     *            Information on the file
     * @param allPieces
     *            Whether or not the instance should specify all possible pieces
     *            or none.
     * @return A new Instance.
     */
    static PiecesSpec newInstance(final FileInfo fileInfo,
            final boolean allPieces) {
        return allPieces
                ? (fileInfo.getPieceCount() == 1
                        ? new PieceSpec(fileInfo, 0)
                        : new FileSpec(fileInfo, true))
                : new FileSpec(fileInfo);
    }

    /**
     * Returns information on the associated file.
     * 
     * @return Information on the associated file.
     */
    final FileInfo getFileInfo() {
        return fileInfo;
    }

    /**
     * Returns the pathname corresponding to this instance.
     * 
     * @return This instance's corresponding pathname.
     */
    Path getPath() {
        return fileInfo.getPath();
    }

    /**
     * Vets information on another instance against this instance in preparation
     * for merging.
     * 
     * @param that
     *            The other instance.
     * @throws IllegalArgumentException
     *             if the other instance is for a different file.
     */
    protected void vetMerger(final PiecesSpec that) {
        if (!fileInfo.equals(that.fileInfo)) {
            throw new IllegalArgumentException(fileInfo.toString() + " != "
                    + that.fileInfo);
        }
    }

    /**
     * Merges this instance with another. The returned instance might be this
     * instance, or the other instance, or a new instance. Instances that aren't
     * returned remain unmodified.
     * 
     * @param that
     *            The other instance.
     * @return The merger of the two instances.
     * @throws NullPointerException
     *             if {@code that == null}.
     */
    abstract PiecesSpec merge(PiecesSpec that);

    /**
     * Merges this instance with a piece-specification. The returned instance
     * might be this instance, or the other instance, or a new instance.
     * Instances that aren't returned remain unmodified.
     * 
     * @param that
     *            Specification of the piece of data.
     * @return The merger of the two instances.
     * @throws NullPointerException
     *             if {@code that == null}.
     */
    abstract PiecesSpec merge(PieceSpec that);

    /**
     * Merges this instance with a partial file-specification. The returned
     * instance might be this instance, or the other instance, or a new
     * instance. Instances that aren't returned remain unmodified.
     * 
     * @param that
     *            The specification of some of the data in a file.
     * @return The merger of the two instances.
     * @throws NullPointerException
     *             if {@code that == null}.
     */
    abstract PiecesSpec merge(FileSpec that);

    /**
     * Has this instance process itself.
     * 
     * @param specProcessor
     *            The processor of data specifications (implements the "what").
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    abstract void processYourself(SpecProcessor specProcessor)
            throws InterruptedException, IOException;
}