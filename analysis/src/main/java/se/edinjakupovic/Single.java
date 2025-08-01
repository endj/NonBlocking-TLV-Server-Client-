package se.edinjakupovic;

import se.edinjakupovic.single_reactor.SingleReactorServer;
import se.edinjakupovic.utils.PayloadUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static se.edinjakupovic.Common.measureNioClientReuse;
import static se.edinjakupovic.Common.printStats;
import static se.edinjakupovic.Common.runWarmup;

public class Single implements Sim {


    @Override
    public void run(Common.CommandLineArgs simArgs) throws Exception {
        System.out.printf("Running SingleReactor simulation: %d warmups, %d workers, %d payloads per id%n", simArgs.warmups(), simArgs.workers(), simArgs.payloads());

        SingleReactorServer server = createServer();

        CountDownLatch latch = new CountDownLatch(1);
        try (ExecutorService serverExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "server"))) {
            serverExecutor.execute(() -> startServer(server, latch));
            if (!latch.await(1L, TimeUnit.SECONDS)) throw new IllegalStateException("Failed to start server");

            runWarmup(simArgs);

            System.out.println("\nStarting test...\n");
            Common.Stats stats = measureNioClientReuse(true, simArgs.clients(), simArgs.simulationName(), simArgs.payloads());
            printStats(simArgs, stats);

            serverExecutor.shutdownNow();
        }
    }


    private static SingleReactorServer createServer() {
        return new SingleReactorServer(new ServerConfig(
                new InetSocketAddress(8080),
                new TLVConfig(
                        5, 1000
                ),
                1000L,
                1000L,
                0,
                1000,
                1,
                Map.of(
                        (byte) 0, _ -> PayloadUtils.payload((byte) 0, "Some result", true)
                ),
                _ -> ServerConstants.ERROR_TYPE_BASE
        ));
    }


    private static void startServer(SingleReactorServer server, CountDownLatch latch) {
        try {
            server.start(latch::countDown);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}