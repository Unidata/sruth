package edu.ucar.unidata.dynaccn;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.Callable;

/**
 * Receives objects from the remote peer and acts upon them.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
public abstract class Receiver<T> implements Callable<Void> {
    /**
     * The local peer.
     */
    protected final Peer   peer;
    /**
     * The type of the received objects.
     */
    private final Class<T> type;

    /**
     * Constructs from the local peer.
     * 
     * @param peer
     *            The local peer.
     * @param type
     *            The type of the received objects.
     * @throws NullPointerException
     *             if {@code peer == null || type == null}.
     */
    protected Receiver(final Peer peer, final Class<T> type) {
        if (null == peer || null == type) {
            throw new NullPointerException();
        }

        this.peer = peer;
        this.type = type;
    }

    /**
     * Reads objects from the connection to the remote peer and processes them.
     * 
     * @throws ClassCastException
     *             if a read object has the wrong type.
     * @throws ClassNotFoundException
     *             if an invalid object is read.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public Void call() throws IOException, ClassNotFoundException,
            InterruptedException {
        final ObjectInputStream objStream = getInputStream(peer);

        try {
            while (process(type.cast(objStream.readObject()))) {
                ;
            }
        }
        catch (final EOFException e) {
            // ignored
        }

        return null;
    }

    /**
     * Returns the appropriate object input stream.
     * 
     * @param peer
     *            The local peer.
     * @return The appropriate object input stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    protected abstract ObjectInputStream getInputStream(Peer peer)
            throws IOException;

    /**
     * Processes a received object.
     * 
     * @param obj
     *            The object to be processed.
     * @return {@code true} if further processing should continue.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    protected abstract boolean process(T obj) throws IOException,
            InterruptedException;
}