package se.edinjakupovic;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static se.edinjakupovic.NioClientContext.Status.*;

public class NioClient {
    private static final Logger log = Logger.getLogger("NioClient");
    private final InetSocketAddress address;
    private final Selector selector;
    private final Thread selectorThread;
    // For keep-alive
    private SocketChannel sharedChannel;
    private SelectionKey sharedKey;
    // requests queue to avoid synchronization on selector
    private final BlockingQueue<NioClientContext> queue = new ArrayBlockingQueue<>(1024);
    public final ClientState state = new ClientState();

    public NioClient(InetSocketAddress address) throws IOException {
        this.address = address;
        this.selector = Selector.open();
        this.selectorThread = new Thread(this::runSelectorLoop, "NIO-Client-Thread");
        selectorThread.start();
    }

    public NioClient() throws IOException {
        this(new InetSocketAddress(8080));
    }

    AtomicInteger count = new AtomicInteger(0);

    public CompletableFuture<Long> sendRPC(ByteBuffer request) {
        boolean keepAlive = (request.get(0) & 0x80) != 0;
        CompletableFuture<Long> response = new CompletableFuture<>();
        return queue.add(new NioClientContext(request, response, keepAlive, count.incrementAndGet()))
                ? response
                : null;
    }

    private void registerRPCRequests() throws IOException {
        NioClientContext request = queue.poll();
        while (request != null) {
            state.requestsRegistered++;
            boolean shouldReuseChannel = request.keepAlive
                    && sharedChannel != null
                    && sharedKey != null
                    && sharedChannel.isConnected()
                    && sharedKey.isValid();

            if (shouldReuseChannel) {
                //print("Reusing channel on request " + request.id + " channels opened" + state.channelsOpened);
                sharedKey = sharedChannel.register(selector, SelectionKey.OP_WRITE, request);
                state.channelsReused++;
            } else {
                state.channelsOpened++;
                //print("Opening new channel " + request.id + " channels opened " + state.channelsOpened);
                SocketChannel channel = SocketChannel.open();
                channel.configureBlocking(false);

                SelectionKey key = channel.connect(address)
                        ? channel.register(selector, SelectionKey.OP_WRITE, request)
                        : channel.register(selector, SelectionKey.OP_CONNECT, request);

                if (request.keepAlive) {
                    sharedChannel = channel;
                    sharedKey = key;
                }
            }
            request = queue.poll();
        }
    }

    private void runSelectorLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                registerRPCRequests();
                selector.selectNow(key -> {
                    try {
                        if (!key.isValid()) {
                            print("Closing invalid key ");
                            closeChannel(key);
                            return;
                        }
                        if (key.isConnectable()) handleConnect(key);
                        if (key.isWritable()) handleWrite(key);
                        if (key.isReadable()) handleRead(key);
                    } catch (IOException e) {
                        if (key.attachment() instanceof NioClientContext nio) {
                            print("EXCEPTION " + e.getMessage() + " on " + nio.id);
                            nio.onError("Unknown caught " + e.getMessage() + " " + nio.elapsed());
                        }
                        closeChannel(key);
                    } catch (CancelledKeyException ce) {
                        if (key.attachment() instanceof NioClientContext nio) {
                            print("CANCELLED KEY " + nio.id);
                            nio.onError("Cancelled key " + nio.elapsed());
                        }
                        closeChannel(key);
                    }
                });
            } catch (IOException e) {
                log.severe("Got error " + e);
            }
        }
        try {
            selector.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        NioClientContext clientContext = (NioClientContext) key.attachment();

        printD("Writing on " + clientContext.id);
        int wrote = channel.write(clientContext.request);
        if (wrote < 0) {
            clientContext.onError("Wrote " + wrote);
            closeChannel(key);
            return;
        }
        if (!clientContext.request.hasRemaining()) {
            key.interestOps(SelectionKey.OP_READ);
            clientContext.status = READING_HEADER;
        }
    }

    private void handleConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        NioClientContext clientContext = (NioClientContext) key.attachment();
        printD("CONNECTING " + clientContext.id);

        try {
            if (!channel.finishConnect()) {
                return;
            }
        } catch (SocketException s) {
            state.channelConnectionErrors++;
            print("Got exception while opening socket " + s.getMessage());
            clientContext.onError("Server rejected connection");
            closeChannel(key);
            return;
        }
        state.connectDurationMs += clientContext.elapsed();
        state.channelConnected++;

        key.interestOps(SelectionKey.OP_WRITE);
        clientContext.status = WRITING;
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        NioClientContext context = (NioClientContext) key.attachment();

        printD("Reading on " + context.id);
        if (context.status == NioClientContext.Status.READING_HEADER) {
            int read = context.readHeader(channel, key);
            if (read < 0) {
                context.onError("Got " + read + " while reading");
                closeChannel(key);
                return;
            }
            if (read == 0) return;
        }
        if (context.status == READING_BODY) {
            int read = channel.read(context.bodyBuffer);
            if (read < 0) {
                context.onError("SERVER CLOSED - handleRead");
                closeChannel(key);
                return;
            }
            if (read == 0) return;
            if (!context.bodyBuffer.hasRemaining()) {
                context.onSuccess();
                state.requestDurationMs += context.elapsed();
                state.requestCompleted++;
                if (context.keepAlive) {
                    sharedKey = key;
                    key.interestOps(SelectionKey.OP_READ);
                    selector.wakeup();
                } else {
                    closeChannel(key);
                }
            }
        }
    }

    private void closeChannel(SelectionKey key) {
        if (key == null) return;
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            key.cancel();
            channel.close();
            if (channel == sharedChannel) {
                sharedChannel = null;
                sharedKey = null;
            }
            state.channelsClosed++;
        } catch (IOException e) {
            log.warning("Error closing channel " + e);
        }
    }

    public void stop() {
        selectorThread.interrupt();
    }

    private static final boolean logEnabled = false;
    private static final boolean dLogEnabled = false;

    public void print(Object any) {
        if (logEnabled) System.out.println("[" + this.state.clientId + "] - " + any);
    }

    public void printD(Object any) {
        if (dLogEnabled) System.out.println("[" + this.state.clientId + "] - " + any);
    }

    public static final class ClientState {
        String clientId = UUID.randomUUID().toString().split("-")[4];
        int channelsOpened = 0;
        int channelsReused = 0;
        int channelsClosed = 0;
        int requestsRegistered = 0;
        int requestCompleted = 0;
        int channelConnected = 0;
        int channelConnectionErrors = 0;
        long requestDurationMs = 0;
        long connectDurationMs = 0;

        @Override
        public String toString() {
            return "reqDurationMs=" + formatDuration(requestDurationMs) +
                    ", reqAvgDuration=" + formatDuration((double) requestDurationMs / requestCompleted) +
                    ", conAvgDuration=" + formatDuration((double) connectDurationMs / requestCompleted) +
                    ", reqCompleted=" + requestCompleted +
                    ", chanOpened=" + channelsOpened +
                    ", chanClosed=" + channelsClosed +
                    ", chanReused=" + channelsReused +
                    ", chanConnected=" + channelConnected +
                    ", chanConnectionErrors=" + channelConnectionErrors +
                    ", clientId='" + clientId + '\'' +
                    ", reqRegistered=" + requestsRegistered;
        }

        private String formatDuration(double ms) {
            if (ms >= 1.0) {
                return String.format("%.2f ms", ms);
            } else if (ms >= 0.001) {
                return String.format("%.2f µs", ms * 1000);
            } else {
                return String.format("%.0f ns", ms * 1_000_000);
            }
        }
    }
}

