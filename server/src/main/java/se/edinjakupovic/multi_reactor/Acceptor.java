package se.edinjakupovic.multi_reactor;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class Acceptor implements Runnable {
    private final ServerSocketChannel serverSocket;
    private final Selector selector;
    private final WorkerReactor[] workers;
    private final AtomicInteger index = new AtomicInteger(0);

    public Acceptor(ServerSocketChannel serverSocket, WorkerReactor[] workers, Runnable onStart) throws IOException {
        this.serverSocket = serverSocket;
        this.selector = Selector.open();
        this.workers = workers;

        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        onStart.run();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                selector.select();
                var selectedKeys = selector.selectedKeys().iterator();
                while (selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();
                    if (key.isAcceptable()) {
                        accept();
                    }
                }
            } catch (IOException e) {
                System.err.println("Acceptor error: " + e.getMessage());
            }
        }
    }

    private void accept() {
        try {
            SocketChannel client = serverSocket.accept();
            if (client != null) {
                client.configureBlocking(false);
                client.setOption(StandardSocketOptions.TCP_NODELAY, true);
                WorkerReactor worker = nextWorker();
                worker.registerNewClient(client);
            }
        } catch (IOException e) {
            System.err.println("Failed to accept connection: " + e.getMessage());
        }
    }

    private WorkerReactor nextWorker() {
        return workers[index.getAndIncrement() % workers.length];
    }
}
