/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
     * Pathname of the file directory.
     */
    private final File                            dirPath;

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
     * @param dirPath
     *            Pathname of the file directory.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code connection == null}.
     */
    Peer(final Connection connection, final File dirPath) throws IOException {
        if (null == connection) {
            throw new NullPointerException();
        }

        this.connection = connection;
        this.dirPath = dirPath;

        // requestSenderFuture =
        completionService.submit(new RequestSender(this));
        // requestReceiverFuture =
        completionService.submit(new RequestReceiver(this));

        // noticeSenderFuture =
        completionService.submit(new NoticeSender(this, dirPath));
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
             * Can't happen because the cancellation only occurs in the previous
             * exception.
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
    void createEmptyFile(final FileInfo fileInfo) throws IOException {
        fileInfo.getFile(dirPath).createNewFile();
    }
}
