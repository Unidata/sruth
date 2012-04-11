/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import net.jcip.annotations.ThreadSafe;

/**
 * A file that will be published
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class PubFile {
    /**
     * The associated archive.
     */
    private final Archive             archive;
    /**
     * The pathname of the hidden file.
     */
    private final Path                hiddenPath;
    /**
     * The byte channel to the file.
     */
    private final SeekableByteChannel channel;

    /**
     * Constructs from the archive and the pathname of the file relative to the
     * root of the archive.
     * 
     * @param archive
     *            The archive.
     * @param path
     *            The pathname of the file relative to the root of the archive.
     * @throws FileAlreadyExistsException
     *             if the hidden version of the file already exists.
     * @throws IOException
     *             if an I/O exception occurs.
     * @throws NullPointerException
     *             if {@code archive == null || hiddenPath == null}.
     */
    PubFile(final Archive archive, final Path path)
            throws FileAlreadyExistsException, IOException {
        if (null == archive || null == path) {
            throw new NullPointerException();
        }
        this.archive = archive;
        this.hiddenPath = archive.getHiddenAbsolutePath(path);
        Files.createDirectories(hiddenPath.getParent());
        channel = Files.newByteChannel(hiddenPath, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
    }

    /**
     * Write a byte-buffer to the file. Throws an exception if called after
     * {@link #publish()}.
     * 
     * @param buf
     *            The byte-buffer to be written.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void write(final ByteBuffer buf) throws IOException {
        channel.write(buf);
    }

    /**
     * Publishes the file. Throws an exception if called twice.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void publish() throws IOException {
        channel.close();
        final Path newPath = archive.getVisiblePath(hiddenPath);
        Files.createDirectories(newPath.getParent());
        Files.move(hiddenPath, newPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }
}
