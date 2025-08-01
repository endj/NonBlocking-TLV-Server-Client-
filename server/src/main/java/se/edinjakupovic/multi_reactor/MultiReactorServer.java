package se.edinjakupovic.multi_reactor;

import se.edinjakupovic.ServerConfig;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

public class MultiReactorServer {

    private final ServerConfig config;
    private Thread acceptorThread;
    private Thread[] reactors;

    public MultiReactorServer(ServerConfig config) {
        this.config = config;
    }

    public void startServer(Runnable onStart) throws IOException {
        int workerCount = config.workers();
        WorkerReactor[] workers = new WorkerReactor[workerCount];

        reactors = new Thread[workerCount];

        for (int i = 0; i < workerCount; i++) {
            WorkerReactor reactor = new WorkerReactor(config);
            workers[i] = reactor;
            reactors[i] =  new Thread(reactor, "reactor-" + i);
            reactors[i].start();
        }

        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.configureBlocking(false);
        serverSocket.socket().bind(config.bindAddress());

        Acceptor acceptor = new Acceptor(serverSocket, workers, onStart);
        acceptorThread = new Thread(acceptor, "acceptor");
        acceptorThread.start();
    }

    public void shutdown() {
        acceptorThread.interrupt();
        for (Thread reactor : reactors) {
            reactor.interrupt();
        }
    }
}
