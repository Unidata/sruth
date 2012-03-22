import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

class SocketExperiment {
    static final AtomicInteger port    = new AtomicInteger();
    static final CyclicBarrier barrier = new CyclicBarrier(2);

    public static void main(final String[] args) throws IOException,
            InterruptedException, BrokenBarrierException,
            ClassNotFoundException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runServer();
                }
                catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        runClient();
    }

    static void runServer() throws IOException, ClassNotFoundException,
            InterruptedException, BrokenBarrierException {
        final Selector selector = Selector.open();
        final ServerSocketChannel servSockChan = ServerSocketChannel.open();
        final ServerSocket servSock = servSockChan.socket();

        servSockChan.configureBlocking(false);
        servSockChan.register(selector, SelectionKey.OP_ACCEPT);
        servSock.bind(new InetSocketAddress(0));

        port.set(servSock.getLocalPort());
        barrier.await();

        for (;;) {
            if (selector.select() > 0) {
                final Iterator<SelectionKey> iter = selector.selectedKeys()
                        .iterator();
                final SelectionKey key = iter.next();
                iter.remove();

                if (key.isAcceptable()) {
                    final SocketChannel sockChan = ((ServerSocketChannel) key
                            .channel()).accept();
                    sockChan.finishConnect();
                    final Socket socket = sockChan.socket();

                    final ObjectInputStream objectInputStream = new ObjectInputStream(
                            socket.getInputStream());
                    final ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                            socket.getOutputStream());

                    final Object obj = objectInputStream.readObject();
                    System.out.println("Server: " + obj);
                    objectOutputStream.writeObject(obj);

                    break;
                }
            }
        }
    }

    static void runClient() throws InterruptedException,
            BrokenBarrierException, UnknownHostException, IOException,
            ClassNotFoundException {
        barrier.await();

        final Socket socket = new Socket(InetAddress.getLocalHost(), port.get());

        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                socket.getOutputStream());
        final ObjectInputStream objectInputStream = new ObjectInputStream(
                socket.getInputStream());

        objectOutputStream.writeObject(new Integer(0));
        final Object obj = objectInputStream.readObject();

        System.out.println("Client: " + obj);
    }
}
