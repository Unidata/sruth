package edu.ucar.unidata.sruth;

import java.io.Serializable;

/**
 * A data-specification comprising one or more pieces of data in a file.
 * 
 * @author Steven R. Emmerson
 */
abstract class FilePieceSpecSet implements PieceSpecSetIface, Serializable {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the file.
     * 
     * @serial
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
    protected FilePieceSpecSet(final FileInfo fileInfo) {
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
    static FilePieceSpecSet newInstance(final FileInfo fileInfo,
            final boolean allPieces) {
        return allPieces
                ? (fileInfo.getPieceCount() == 1
                        ? new PieceSpec(fileInfo, 0)
                        : new FilePieceSpecs(fileInfo, true))
                : new FilePieceSpecs(fileInfo);
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
     * Returns the associated file's identifier.
     * 
     * @return The identifier of the associated file.
     */
    FileId getFileId() {
        return fileInfo.getFileId();
    }

    /**
     * Returns the archive pathname corresponding to this instance.
     * 
     * @return This instance's corresponding archive pathname.
     */
    ArchivePath getArchivePath() {
        return fileInfo.getPath();
    }

    @Override
    public FilePieceSpecSet clone() {
        try {
            return (FilePieceSpecSet) super.clone();
        }
        catch (final CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileInfo == null)
                ? 0
                : fileInfo.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FilePieceSpecSet other = (FilePieceSpecSet) obj;
        if (fileInfo == null) {
            if (other.fileInfo != null) {
                return false;
            }
        }
        else if (!fileInfo.equals(other.fileInfo)) {
            return false;
        }
        return true;
    }
}