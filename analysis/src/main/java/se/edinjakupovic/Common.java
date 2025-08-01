package se.edinjakupovic;

import se.edinjakupovic.utils.PayloadUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Common {
    public record Stats(long durationMs, int success, int fails, int timeouts) {
    }


    public static Stats measureNioClientReuse(boolean saveResult, int clients, String simulationName, int payloads) throws InterruptedException, IOException {
        @SuppressWarnings("resource")
        ExecutorService executorService = Executors.newFixedThreadPool(clients);
        List<NioClient> nioClients = new ArrayList<>(clients);
        List<MakeCallsTask> tasks = IntStream.range(0, clients).mapToObj(w -> {
            try {
                NioClient nioClient = new NioClient();
                nioClients.add(nioClient);
                return new MakeCallsTask(nioClient, w, payloads);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        List<TaskStats> stats = new ArrayList<>(clients);
        long start = System.nanoTime();
        List<Future<TaskStats>> futures = executorService.invokeAll(tasks);
        futures.forEach(f -> {
            try {
                stats.add(f.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        long stop = System.nanoTime();
        var duration = TimeUnit.NANOSECONDS.toMillis(stop - start);
        nioClients.forEach(NioClient::stop);

        int success = 0, failed = 0, timeouts = 0;
        for (TaskStats stat : stats) {
            success += stat.success;
            failed += stat.failed;
            timeouts += stat.timeouts;
        }

        if (saveResult) {
            saveStats(clients, simulationName, payloads, stats);
        }

        executorService.shutdownNow();
        return new Stats(duration, success, failed, timeouts);
    }


    private static void saveStats(int clients, String simulationName, int payloads, List<TaskStats> stats) throws IOException {
        var result = taskStatsToCsv(stats);
        String name = simulationName + "_" + LocalDateTime.now() + "_WORKERS_%d_PAYLOADS_%d_.csv".formatted(clients, payloads);
        Path filePath = Path.of("results/" + name);
        System.out.println("Writing result to " + filePath);
        Files.writeString(filePath, result);
    }


    record MakeCallsTask(NioClient nioClient, int id, int payloads) implements Callable<TaskStats> {

        @Override
        public TaskStats call() throws Exception {
            int success = 0, failed = 0, timeouts = 0;

            for (int i = 0; i < payloads; i++) {
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

            System.out.println("W:" + id + ", " + nioClient.state);
            return new TaskStats(success, failed, timeouts, nioClient.state, id);
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

    private static String taskStatsToCsv(List<TaskStats> stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("worker_id,success,failed,timeouts,channelsOpened,channelsReused,channelsClosed,requestsRegistered,requestCompleted,channelConnected,channelConnectionErrors,requestDurationMs,connectDurationMs\n");
        for (TaskStats stat : stats) {
            sb.append(stat.toCSV()).append("\n");
        }
        return sb.toString();
    }

    public static void runWarmup(Common.CommandLineArgs simArgs) {
        IntStream.range(0, simArgs.warmups()).forEach(round -> {
            System.out.println("\nRunning warmup " + round);
            try {
                measureNioClientReuse(false, simArgs.clients(), simArgs.simulationName(), simArgs.payloads());
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public static void printStats(CommandLineArgs simArgs, Stats stats) {
        var numberOfCalls = simArgs.payloads() * simArgs.clients();

        var okCallsPerMs = stats.success() / stats.durationMs();
        var okCallsPerSec = stats.success() / ((double) stats.durationMs() / 1000);

        var failCallsPerMs = stats.fails() / stats.durationMs();
        var failCallsPerSec = stats.fails() / ((double) stats.durationMs() / 1000);

        String results = """
                
                Done: %d calls in %dms
                
                Ok:
                
                %d count
                %d calls/ms
                %.2f calls/s
                %.2f calls/client/s
                
                Errors:
                
                %d count
                %d calls/ms
                %.2f calls/s
                
                Timeouts:
                
                %d count
                
                
                %n""".formatted(
                numberOfCalls,
                stats.durationMs(),

                stats.success(),
                okCallsPerMs,
                okCallsPerSec,
                okCallsPerSec / simArgs.clients,

                stats.fails(),
                failCallsPerMs,
                failCallsPerSec,
                stats.timeouts());
        System.out.printf(results);
    }


    public static CommandLineArgs parseArgs(String[] args) {
        int workers = 0;
        int payloads = 0;
        int warmups = 0;
        int clients = 0;
        String simulationName = "";
        String strategy = "single";

        for (String arg : args) {
            if (arg.startsWith("--workers=")) {
                workers = parseUnsignedInt(arg, "--workers=");
            } else if(arg.startsWith("--clients=")) {
              clients = parseUnsignedInt(arg, "--clients=");
            } else if (arg.startsWith("--payloads=")) {
                payloads = parseUnsignedInt(arg, "--payloads=");
            } else if (arg.startsWith("--warmups=")) {
                warmups = parseUnsignedInt(arg, "--warmups=");
            } else if (arg.startsWith("--name=")) {
                simulationName = arg.substring("--name=".length()).replaceAll("\\s+", "_");
            } else if (arg.startsWith("--strategy=")) {
                strategy = arg.substring("--strategy=".length());
            } else {
                throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (workers <= 0 || payloads <= 0) {
            throw new IllegalArgumentException("--workers and --payloads must be positive integers");
        }
        if (!strategy.equals("single") && !strategy.equals("multi")) {
            throw new IllegalArgumentException("--strategy either 'single' or 'multi' for patterns, got [" + strategy + "]");
        }

        return new CommandLineArgs(workers, clients, payloads, warmups, simulationName, strategy);
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

    public record CommandLineArgs(int workers, int clients, int payloads, int warmups, String simulationName, String strategy) {
    }

}
