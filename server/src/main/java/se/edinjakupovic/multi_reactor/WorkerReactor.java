package se.edinjakupovic.multi_reactor;

import se.edinjakupovic.ClientStatus;
import se.edinjakupovic.MessageHandler;
import se.edinjakupovic.ServerClientContext;
import se.edinjakupovic.ServerConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

public class WorkerReactor implements Runnable {
    private static final Logger log = Logger.getLogger("WorkerReactor");

    private final Selector selector;
    private final Queue<SocketChannel> newClients = new ConcurrentLinkedQueue<>();
    private final Map<Byte, MessageHandler> handlers;
    private final MessageHandler errorHandler;
    private final int headerSize;

    public WorkerReactor(ServerConfig config) throws IOException {
        this.selector = Selector.open();
        this.handlers = config.handlers();
        this.errorHandler = config.errorHandler();
        this.headerSize = config.config().headerSizeBytes();
    }

    public void registerNewClient(SocketChannel client) {
        newClients.add(client);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                selector.select();

                registerPendingClients();

                var keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    try {
                        if (key.isReadable()) handleRead(key);
                        if (key.isWritable()) handleWrite(key);
                    } catch (IOException e) {
                        closeChannel(key);
                    }
                }
            } catch (IOException e) {
                log.severe("Selector error: " + e.getMessage());
            }
        }
    }

    private void registerPendingClients() throws IOException {
        SocketChannel client;
        //noinspection resource
        while ((client = newClients.poll()) != null) {
            ServerClientContext ctx = new ServerClientContext(headerSize);
            log.info("Reactor " + Thread.currentThread().getName() + " handled client ");
            client.register(selector, SelectionKey.OP_READ, ctx);
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ServerClientContext ctx = (ServerClientContext) key.attachment();

        if (ctx.status == ClientStatus.READING_HEADER) {
            int read = ctx.readHeader(channel, key);
            if (read < 0) {
                closeChannel(key);
                return;
            }
            if (read == 0) return;
        }

        if (ctx.status == ClientStatus.READING_BODY) {
            int read = channel.read(ctx.bodyBuffer);
            if (read < 0) {
                closeChannel(key);
                return;
            }
            if (read == 0) return;
            if (!ctx.bodyBuffer.hasRemaining()) {
                processMessage(key, ctx.tlvType, ctx.bodyBuffer);
            }
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ServerClientContext ctx = (ServerClientContext) key.attachment();

        int written = channel.write(ctx.responseBuffer);
        if (written < 0) {
            closeChannel(key);
            return;
        }
        if (!ctx.responseBuffer.hasRemaining()) {
            if (ctx.keepAlive) {
                ctx.resetCtx();
                key.interestOps(SelectionKey.OP_READ);
            } else {
                closeChannel(key);
            }
        }
    }

    private void processMessage(SelectionKey key, byte type, ByteBuffer bodyBuffer) {
        MessageHandler handler = handlers.getOrDefault(type, errorHandler);
        ByteBuffer response = handler.processMessage(bodyBuffer);
        ServerClientContext ctx = (ServerClientContext) key.attachment();
        ctx.setResponse(response);
        key.interestOps(SelectionKey.OP_WRITE);
    }

    private void closeChannel(SelectionKey key) {
        try {
            key.cancel();
            key.channel().close();
        } catch (IOException e) {
            log.warning("Failed to close client channel: " + e.getMessage());
        }
    }
}
