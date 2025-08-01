package se.edinjakupovic;

import se.edinjakupovic.multi_reactor.MultiReactorServer;
import se.edinjakupovic.utils.PayloadUtils;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static se.edinjakupovic.Common.measureNioClientReuse;
import static se.edinjakupovic.Common.printStats;
import static se.edinjakupovic.Common.runWarmup;

public class Multi implements Sim {

    @Override
    public void run(Common.CommandLineArgs simArgs) throws Exception {
        System.out.printf("Running MultiReactor simulation: %d warmups, %d workers, %d clients, %d payloads per id%n", simArgs.warmups(), simArgs.workers(), simArgs.clients(), simArgs.payloads());
        MultiReactorServer multiReactorServer = createServer(simArgs);
        CountDownLatch latch = new CountDownLatch(1);
        multiReactorServer.startServer(latch::countDown);
        if (!latch.await(1, TimeUnit.SECONDS)) throw new IllegalStateException("Failed to start server");

        runWarmup(simArgs);

        System.out.println("\nStarting test...\n");
        Common.Stats stats = measureNioClientReuse(true, simArgs.clients(), simArgs.simulationName(), simArgs.payloads());
        printStats(simArgs, stats);

        multiReactorServer.shutdown();
    }

    private static MultiReactorServer createServer(Common.CommandLineArgs args) {
        return new MultiReactorServer(
                new ServerConfig(
                        new InetSocketAddress(8080),
                        new TLVConfig(
                                5, 1000
                        ),
                        1000L,
                        1000L,
                        0,
                        1000,
                        args.workers(),
                        Map.of(
                                (byte) 0, _ -> PayloadUtils.payload((byte) 0, "Some result", true)
                        ),
                        _ -> ServerConstants.ERROR_TYPE_BASE
                )
        );
    }


}
