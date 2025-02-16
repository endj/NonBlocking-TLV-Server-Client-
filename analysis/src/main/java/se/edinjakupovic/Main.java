package se.edinjakupovic;

import se.edinjakupovic.utils.PayloadUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Main {

    private static int WORKERS = 1;
    private static int PAYLOADS_PER_WORKER = 1_000_000;
    private static String SIMULATION_NAME = "";

    record Stats(long durationMs, int success, int fails, int timeouts) {
    }

    public static void main(String[] args) throws Exception {
        CommandLineArgs commandLineArgs = parseArgs(args);
        WORKERS = commandLineArgs.workers;
        PAYLOADS_PER_WORKER = commandLineArgs.payloads;
        SIMULATION_NAME = commandLineArgs.simulationName;
        int CALLS = WORKERS * PAYLOADS_PER_WORKER;

        System.out.printf("Running simulation: %d warmups, %d workers, %d payloads per worker%n", commandLineArgs.warmups, WORKERS, PAYLOADS_PER_WORKER);


        Server server = new Server(new ServerConfig(
                new InetSocketAddress(8080),
                new TLVConfig(
                        5, 1000
                ),
                1000L,
                1000L,
                0,
                1000,
                Map.of(
                        (byte) 0, _ -> PayloadUtils.payload((byte) 0, "Some result", true)
                ),
                _ -> ServerConstants.ERROR_TYPE_BASE
        ));
        CountDownLatch latch = new CountDownLatch(1);
        var threadName = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Server");
            }
        };
        try (ExecutorService serverExecutor = Executors.newSingleThreadExecutor(threadName)) {
            serverExecutor.execute(() -> startServer(server, latch));
            if (!latch.await(1L, TimeUnit.SECONDS)) throw new IllegalStateException("Failed to start server");


            // Warmup
            IntStream.range(0, commandLineArgs.warmups).forEach(round -> {
                System.out.println("\nRunning warmup " + round);
                try {
                    measureNioClientReuse(false);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(1000);
            System.out.println("\nRunning test...\n");
            Stats stats = measureNioClientReuse(true);

            var okCallsPerMs = stats.success / stats.durationMs;
            var okCallsPerSec = stats.success / ((double) stats.durationMs / 1000);

            var failCallsPerMs = stats.fails / stats.durationMs;
            var failCallsPerSec = stats.fails / ((double) stats.durationMs / 1000);

            String results = """
                    
                    Done: %d calls in %dms
                    
                    Ok:
                    
                    %d count
                    %d calls/ms
                    %.2f calls/s
                    %.2f calls/worker/s
                    
                    Errors:
                    
                    %d count
                    %d calls/ms
                    %.2f calls/s
                    
                    Timeouts:
                    
                    %d count
                    
                    
                    %n""".formatted(
                    CALLS,
                    stats.durationMs,

                    stats.success,
                    okCallsPerMs,
                    okCallsPerSec,
                    okCallsPerSec / WORKERS,

                    stats.fails,
                    failCallsPerMs,
                    failCallsPerSec,
                    stats.timeouts);
            System.out.printf(results);
            serverExecutor.shutdownNow();
        }
    }

    private static Stats measureNioClientReuse(boolean saveResult) throws InterruptedException, IOException {
        @SuppressWarnings("resource")
        ExecutorService executorService = Executors.newFixedThreadPool(WORKERS);
        List<NioClient> clients = new ArrayList<>();
        List<MakeCallsTask> tasks = IntStream.range(0, WORKERS).mapToObj(w -> {
            try {
                NioClient nioClient = new NioClient();
                clients.add(nioClient);
                return new MakeCallsTask(nioClient, w);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        List<TaskStats> stats = new ArrayList<>();
        long start, stop;

        start = System.nanoTime();
        List<Future<TaskStats>> futures = executorService.invokeAll(tasks);
        futures.forEach(f -> {
            try {
                stats.add(f.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        stop = System.nanoTime();
        var duration = TimeUnit.NANOSECONDS.toMillis(stop - start);
        clients.forEach(NioClient::stop);

        int success = 0;
        int failed = 0;
        int timeouts = 0;
        for (TaskStats stat : stats) {
            success += stat.success;
            failed += stat.failed;
            timeouts += stat.timeouts;
        }

        if (saveResult) {
            var result = taskStatsToCsv(stats);
            String name = SIMULATION_NAME + "_" + LocalDateTime.now() + "_WORKERS_%d_PAYLOADS_%d_.csv".formatted(WORKERS, PAYLOADS_PER_WORKER);
            Path filePath = Path.of("results/" + name);
            System.out.println("Writing result to " + filePath);
            Files.writeString(filePath, result);
        }

        executorService.shutdownNow();
        return new Stats(duration, success, failed, timeouts);
    }

    private static String taskStatsToCsv(List<TaskStats> stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("worker_id,success,failed,timeouts,channelsOpened,channelsReused,channelsClosed,requestsRegistered,requestCompleted,channelConnected,channelConnectionErrors,requestDurationMs,connectDurationMs\n");
        for (TaskStats stat : stats) {
            sb.append(stat.toCSV()).append("\n");
        }
        return sb.toString();
    }

    record MakeCallsTask(NioClient nioClient, int worker) implements Callable<TaskStats> {

        @Override
        public TaskStats call() throws Exception {
            int success = 0;
            int failed = 0;
            int timeouts = 0;
            for (int i = 0; i < PAYLOADS_PER_WORKER; i++) {
                CompletableFuture<Long> response = nioClient.sendRPC(PayloadUtils.payload((byte) 0, "Hello", true));
                if (response == null) {
                    failed++;
                    continue;
                }
                try {
                    response.get(1000, TimeUnit.MILLISECONDS);
                    if (response.isCompletedExceptionally()) {
                        failed++;
                    }
                    if (response.isDone()) {
                        success++;
                    }
                } catch (TimeoutException e) {
                    failed++;
                }
            }

            System.out.println("W:" + worker + ", " + nioClient.state);
            return new TaskStats(success, failed, timeouts, nioClient.state, worker);
        }
    }

    record TaskStats(int success, int failed, int timeouts, NioClient.ClientState status, int worker) {

        private String toCSV() {
            return Stream.of(
                    worker,
                    success,
                    failed,
                    timeouts,
                    status.channelsOpened,
                    status.channelsReused,
                    status.channelsClosed,
                    status.requestsRegistered,
                    status.requestCompleted,
                    status.channelConnected,
                    status.channelConnectionErrors,
                    status.requestDurationMs,
                    status.connectDurationMs
            ).map(String::valueOf).collect(Collectors.joining(","));
        }
    }

    private static void startServer(Server server, CountDownLatch latch) {
        try {
            server.start(latch::countDown);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CommandLineArgs parseArgs(String[] args) {
        int workers = 0;
        int payloads = 0;
        int warmups = 0;
        String simulationName = "";

        for (String arg : args) {
            if (arg.startsWith("--workers=")) {
                workers = parseUnsignedInt(arg, "--workers=");
            } else if (arg.startsWith("--payloads=")) {
                payloads = parseUnsignedInt(arg, "--payloads=");
            } else if (arg.startsWith("--warmups=")) {
                warmups = parseUnsignedInt(arg, "--warmups=");
            } else if (arg.startsWith("--name=")) {
                simulationName = arg.substring("--name=".length()).replaceAll("\\s+", "_");
            } else {
                throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (workers <= 0 || payloads <= 0) {
            throw new IllegalArgumentException("--workers and --payloads must be positive integers");
        }

        return new CommandLineArgs(workers, payloads, warmups, simulationName);
    }

    private static int parseUnsignedInt(String arg, String prefix) {
        String valueStr = arg.substring(prefix.length());
        try {
            int value = Integer.parseUnsignedInt(valueStr);
            if (value < 0) {
                throw new IllegalArgumentException(prefix + " must be a non-negative integer");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(prefix + " must be a valid non-negative integer");
        }
    }

    public record CommandLineArgs(int workers, int payloads, int warmups, String simulationName) {
    }

}