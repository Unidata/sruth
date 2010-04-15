/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;

/**
 * Sends notices of available data to a remote peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class NoticeSender extends Sender {
    /**
     * Iterates over the pieces of regular files in a directory. Doesn't
     * recurse.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private static class FileIterator implements Iterator<FileInfo> {
        /**
         * The canonical size, in bytes, of a piece of a file.
         */
        private final int    PIECE_SIZE = 0x20000; // 131072 bytes
        /**
         * The list of files in the directory.
         */
        private final File[] filePaths;
        /**
         * The index of the next pathname to use.
         */
        private int          index;

        /**
         * Constructs from the pathname of the directory that contains the
         * files.
         * 
         * @param dirPath
         *            The pathname of the directory.
         */
        FileIterator(final File dirPath) {
            filePaths = dirPath.listFiles(new FileFilter() {
                @Override
                public boolean accept(final File pathname) {
                    return pathname.isFile();
                }
            });
        }

        @Override
        public boolean hasNext() {
            return filePaths.length > index;
        }

        @Override
        public FileInfo next() {
            final File path = filePaths[index++];

            return new FileInfo(new FileId(new File(path.getName())), path
                    .length(), PIECE_SIZE);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * The pathname of the directory that contains the potential files to be
     * sent.
     */
    private final File dirPath;

    /**
     * Constructs from the local peer and a directory pathname.
     * 
     * @param peer
     *            The local peer.
     * @param dirPath
     *            Pathname of the directory that contains the potential files to
     *            be sent.
     * @throws NullPointerException
     *             if {@code peer == null || dirPath == null}.
     */
    NoticeSender(final Peer peer, final File dirPath) throws IOException {
        super(peer);

        if (null == dirPath) {
            throw new NullPointerException();
        }

        this.dirPath = dirPath;
    }

    @Override
    public Void call() throws IOException {
        final ObjectOutputStream objStream = peer.getNoticeOutputStream();
        final Iterator<FileInfo> fileIter = new FileIterator(dirPath);
        Notice notice;

        while (fileIter.hasNext()) {
            final FileInfo fileInfo = fileIter.next();
            notice = new FileNotice(fileInfo);

            System.out.println("Sending notice: " + notice);
            objStream.writeObject(notice);

            final Iterator<PieceInfo> pieceIter = fileInfo
                    .getPieceInfoIterator();

            while (pieceIter.hasNext()) {
                notice = new PieceNotice(pieceIter.next());

                System.out.println("Sending notice: " + notice);
                objStream.writeObject(notice);
            }
        }

        notice = DoneNotice.INSTANCE;
        System.out.println("Sending notice: " + notice);
        objStream.writeObject(notice);

        objStream.flush();

        return null;
    }
}
