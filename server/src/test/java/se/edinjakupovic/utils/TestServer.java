package se.edinjakupovic.utils;

import se.edinjakupovic.*;
import se.edinjakupovic.single_reactor.SingleReactorServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestServer implements AutoCloseable {
    SingleReactorServer server;
    ExecutorService executor;

    public static TestServer withHandler(Byte type, MessageHandler handler) {
        return new TestServer(Map.of(type, handler));
    }

    public static TestServer withHandlers(Map<Byte, MessageHandler> handlers) {
        return new TestServer(handlers);
    }

    private TestServer(Map<Byte, MessageHandler> handlers) {
        this.server = new SingleReactorServer(new ServerConfig(
                new InetSocketAddress(8080),
                new TLVConfig(
                        5, 1000
                ),
                1000L,
                1000L,
                0,
                1000,
                1,
                handlers,
                _ -> ServerConstants.ERROR_TYPE_BASE
        ));
        start();
    }


    public SimpleClient testClient() {
        return new SimpleClient(new InetSocketAddress(8080));
    }
    private void start() {
        CountDownLatch latch = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            try {
                server.start(latch::countDown);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            if (!latch.await(1, TimeUnit.SECONDS))
                throw new RuntimeException("Server did not start in 1 second");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
