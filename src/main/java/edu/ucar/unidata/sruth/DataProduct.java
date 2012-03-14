/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.jcip.annotations.ThreadSafe;

/**
 * A data-product.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class DataProduct {
    /**
     * The absolute pathname of the root-directory of the archive.
     */
    private final Path     archivePath;
    /**
     * Information on the file.
     */
    private final FileInfo fileInfo;

    /**
     * Constructs from the absolute pathname of the root-directory of the
     * archive and the pathname of the file relative to the root-directory.
     * 
     * @param archivePath
     *            The pathname of the root-directory of the archive.
     * @param fileInfo
     *            Information on the file.
     * @throws IllegalArgumentException
     *             if {@code archivePath} isn't absolute.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code archivePath == null || fileInfo == null}.
     */
    DataProduct(final Path archivePath, final FileInfo fileInfo) {
        if (!archivePath.isAbsolute()) {
            throw new IllegalArgumentException("Not absolute: " + archivePath);
        }
        if (fileInfo == null) {
            throw new NullPointerException();
        }
        this.archivePath = archivePath;
        this.fileInfo = fileInfo;
    }

    /**
     * Returns information on the associated file.
     * 
     * @return Information on the file.
     */
    FileInfo getFileInfo() {
        return fileInfo;
    }

    /**
     * Returns the size of the data-product in bytes.
     * 
     * @return the size of the data-product in bytes.
     */
    long size() {
        return fileInfo.getSize();
    }

    /**
     * Indicates if this instance matches a pattern.
     * 
     * @param pattern
     *            The pattern to be matched.
     * @return {@code true} if and only if this instance matches the pattern.
     */
    boolean matches(final Pattern pattern) {
        return fileInfo.matches(pattern);
    }

    /**
     * Returns a {@link Matcher} for this instance based on a pattern.
     * 
     * @param pattern
     *            The pattern
     * @return The corresponding {@link Matcher}.
     */
    Matcher matcher(final Pattern pattern) {
        return fileInfo.matcher(pattern);
    }

    /**
     * Returns the absolute pathname of the file that contains the data-product.
     * 
     * @return the absolute pathname of the file that contains the data-product.
     */
    File getAbsolutePath() {
        final Path path = fileInfo.getAbsolutePath(archivePath);
        return new File(path.toString());
    }

    /**
     * Returns a read-only I/O channel to this instance's data.
     * 
     * @return a read-only I/O channel to this instance's data.
     * @throws IOException
     *             if an I/O error occurs.
     */
    SeekableByteChannel getReadonlyChannel() throws IOException {
        final Path path = fileInfo.getAbsolutePath(archivePath);
        return Files.newByteChannel(path, StandardOpenOption.READ);
    }

    /**
     * Returns
     */

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "DataProduct [archivePath=" + archivePath + ", fileInfo="
                + fileInfo + "]";
    }
}
