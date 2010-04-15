/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * Exchanges data with its remote counterpart.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Peer implements Callable<Void> {
    /**
     * The task service used by this instance.
     */
    private final ExecutorService                 executorService   = Executors
                                                                            .newCachedThreadPool();
    /**
     * The task completion service used by this instance.
     */
    private final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                            executorService);
    /**
     * The connection with the remote peer.
     */
    private final Connection                      connection;
    /**
     * Pathname of the directory containing files to be sent.
     */
    private final File                            outDir;
    /**
     * Pathname of the directory into which to put received files.
     */
    private final File                            inDir;
    /**
     * Request queue.
     */
    private final BlockingQueue<PieceInfo>        requestQueue      = new SynchronousQueue<PieceInfo>();

    /**
     * The futures of the various tasks.
     */
    // private final Future<Void> requestSenderFuture;
    // private final Future<Void> requestReceiverFuture;
    // private final Future<Void> noticeSenderFuture;
    // private final Future<Void> noticeReceiverFuture;
    /**
     * Constructs from a connection to a remote peer.
     * 
     * @param connection
     *            The connection to the remote peer.
     * @param outDir
     *            Pathname of the directory containing files to be sent.
     * @param inDir
     *            Pathname of the directory into which to put received files.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code connection == null || outDir == null || inDir ==
     *             null}.
     */
    Peer(final Connection connection, final File outDir, final File inDir)
            throws IOException {
        if (null == connection || null == outDir || null == inDir) {
            throw new NullPointerException();
        }

        this.connection = connection;
        this.outDir = outDir;
        this.inDir = inDir;

        // requestSenderFuture =
        completionService.submit(new RequestSender(this));
        // requestReceiverFuture =
        completionService.submit(new RequestReceiver(this));

        // noticeSenderFuture =
        completionService.submit(new NoticeSender(this, outDir));
        // noticeReceiverFuture =
        completionService.submit(new NoticeReceiver(this));
    }

    @Override
    public Void call() throws InterruptedException, ExecutionException {
        try {
            for (int i = 0; i < 4; ++i) {
                completionService.take().get();
            }
        }
        catch (final ExecutionException e) {
            executorService.shutdownNow();
            throw e;
        }
        catch (final CancellationException e) {
            /*
             * Can't happen because cancellation only occurs in the previous
             * exception-handler.
             */
        }

        return null;
    }

    /**
     * Returns the object output stream for notices.
     * 
     * @return The object output stream for notices.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectOutputStream getNoticeOutputStream() throws IOException {
        return connection.getNoticeOutputStream();
    }

    /**
     * Returns the object output stream for requests.
     * 
     * @return The object output stream for requests.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectOutputStream getRequestOutputStream() throws IOException {
        return connection.getRequestOutputStream();
    }

    /**
     * Returns the object input stream for notices.
     * 
     * @return The object input stream for notices.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectInputStream getNoticeInputStream() throws IOException {
        return connection.getNoticeInputStream();
    }

    /**
     * Returns the object input stream for requests.
     * 
     * @return The object input stream for requests.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectInputStream getRequestInputStream() throws IOException {
        return connection.getRequestInputStream();
    }

    /**
     * Creates an empty file based on information about the file.
     * 
     * @param fileInfo
     *            Information about the file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void createFile(final FileInfo fileInfo) throws IOException {
        fileInfo.getFile(inDir).createNewFile();
    }

    /**
     * Saves a piece of data in the file.
     * 
     * @param piece
     *            The piece to be saved.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void save(final Piece piece) throws IOException {
        final File path = piece.getFile(inDir);

        if (!path.exists()) {
            throw new FileNotFoundException("File doesn't exist: \"" + path
                    + "\"");
        }

        final RandomAccessFile file = new RandomAccessFile(
                piece.getFile(inDir), "rwd");

        try {
            file.seek(piece.getOffset());
            file.write(piece.getData());
        }
        finally {
            file.close();
        }
    }

    /**
     * Processes information about an available piece of data at the remote
     * peer.
     * 
     * @param pieceInfo
     *            Information about the available piece of data at the remote
     *            peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final PieceInfo pieceInfo) throws InterruptedException {
        requestQueue.put(pieceInfo);
    }

    /**
     * Returns information on the next wanted piece of data.
     * 
     * @return Information on the next wanted piece of data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    PieceInfo getNextWantedPiece() throws InterruptedException {
        return requestQueue.take();
    }
}
