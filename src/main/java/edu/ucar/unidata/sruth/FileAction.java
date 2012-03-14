/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.jcip.annotations.NotThreadSafe;

import org.slf4j.Logger;

/**
 * Files a data-product.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
public final class FileAction extends Action {
    /**
     * Pathname of the destination file. May contain references to capturing
     * groups.
     */
    private final String        path;
    /**
     * The logger.
     */
    private static final Logger logger = Util.getLogger();

    /**
     * Constructs from the pathname of the destination file.
     * 
     * @param path
     *            Pathname of the destination file. May contain references to
     *            the subsequences of capturing groups matched by
     *            {@link DataProduct#matcher(Pattern)} of the form "$i", where
     *            "i" is the i-th capturing group.
     * @throws NullPointerException
     *             if {@code path == null}.
     */
    public FileAction(final String path) {
        if (path == null) {
            throw new NullPointerException();
        }
        this.path = path;
    }

    @Override
    protected void execute(final Matcher matcher, final DataProduct dataProduct)
            throws IOException {
        assert dataProduct.matches(matcher.pattern());
        final Replacer replacer = getReplacer(matcher);
        final String pathname = replacer.replace(path);
        final Path destPath = Paths.get(pathname);
        write(dataProduct, destPath);
    }

    /**
     * Writes a data-product to a file.
     * 
     * @param dataProduct
     *            The data-product to be written to a file.
     * @param destPath
     *            The pathname of the file into which to write the data-product.
     * @throws IOException
     *             if an I/O error occurs. The destination file will be deleted
     *             if possible.
     */
    private void write(final DataProduct dataProduct, final Path destPath)
            throws IOException {
        Files.createDirectories(destPath.getParent());
        try {
            final FileChannel outChannel = FileChannel.open(destPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                final SeekableByteChannel inChannel = dataProduct
                        .getReadonlyChannel();
                try {
                    logger.info("Filing {}", destPath);
                    outChannel.transferFrom(inChannel, 0, dataProduct.size());
                }
                finally {
                    inChannel.close();
                }
            }
            finally {
                outChannel.close();
            }
        }
        catch (final IOException e) {
            try {
                Files.delete(destPath);
            }
            catch (final IOException ignored) {
            }
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FileAction [path=" + path + "]";
    }
}
