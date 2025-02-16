package se.edinjakupovic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.logging.Logger;

public class Server {
    private static final Logger log = Logger.getLogger("Server");
    private final ServerConfig config;
    private final Map<Byte, MessageHandler> handlers;
    private final MessageHandler errorHandler;

    public Server(ServerConfig config) {
        this.config = config;
        this.handlers = config.handlers();
        this.errorHandler = config.errorHandler();
    }

    public void start(Runnable onStart) throws IOException {
        try (Selector selector = Selector.open();
             ServerSocketChannel serverSocket = ServerSocketChannel.open()) {
            serverSocket.socket().setReuseAddress(true);
            serverSocket.bind(config.bindAddress());
            serverSocket.configureBlocking(false);
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);
            log.info("Started server on port: " + config.bindAddress().getPort());
            onStart.run();

            while (!Thread.currentThread().isInterrupted()) {
                selector.select(key -> {
                    try {
                        if (!key.isValid()) {
                            print("Closing invalid key");
                            closeChannel(key);
                            return;
                        }
                        if (key.isAcceptable()) accept(selector, serverSocket);
                        if (key.isReadable()) read(key);
                        if (key.isWritable()) write(key);
                    } catch (IOException e) {
                        print("SERVER ERROR " + e);
                        closeChannel(key);
                    }
                });
            }
        }
        if (Thread.currentThread().isInterrupted()) print("Server stopped");
    }

    int count = 0;

    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ServerClientContext clientCtx = (ServerClientContext) key.attachment();

        int write = channel.write(clientCtx.responseBuffer);
        if (write < 0) {
            closeChannel(key);
            return;
        }
        if (!clientCtx.responseBuffer.hasRemaining()) {
            print("Finished writing to client " + count);
            if (clientCtx.keepAlive) {
                clientCtx.resetCtx();
                print("Resetting client " + count++);
                key.interestOps(SelectionKey.OP_READ);
            } else {
                closeChannel(key);
            }
        } else {
            print("Wrote " + write + " to client " + count);
            key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ServerClientContext state = (ServerClientContext) key.attachment();
        print("Reading " + count);
        try {
            if (state.status == ClientStatus.READING_HEADER) {
                int read = state.readHeader(clientChannel, key);
                if (read < 0) {
                    print("SERVER ERROR - Got 1 while reading header, closing connection");
                    closeChannel(key);
                    return;
                }
                if (read == 0) return;
            }

            if (state.status == ClientStatus.READING_BODY) {
                int read = clientChannel.read(state.bodyBuffer);
                if (read < 0) {
                    print("SERVER ERROR - Got 1 while reading body, closing connection");
                    closeChannel(key);
                    return;
                }
                if (read == 0) return;
                if (!state.bodyBuffer.hasRemaining()) {
                    print("Processing client " + count);
                    processMessage(key, state.tlvType, state.bodyBuffer);
                }
            }
        } catch (IOException e) {
            log.severe("Server read error: " + e.getMessage());
            closeChannel(key);
        } catch (Exception e) {
            log.severe("Unexpected error during read: " + e.getMessage());
            closeChannel(key);
        }
    }

    private void processMessage(SelectionKey key, byte type, ByteBuffer bodyBuffer) {
        MessageHandler messageHandler = handlers.get(type);
        ByteBuffer responseBuffer = messageHandler == null
                ? errorHandler.processMessage(bodyBuffer)
                : messageHandler.processMessage(bodyBuffer);
        ServerClientContext state = (ServerClientContext) key.attachment();
        state.setResponse(responseBuffer);
        key.interestOps(SelectionKey.OP_WRITE);
    }

    private void accept(Selector selector, ServerSocketChannel serverSocket) throws IOException {
        SocketChannel client = serverSocket.accept();
        if (client == null) return;
        client.configureBlocking(false);
        ServerClientContext clientCtx = new ServerClientContext(config.config().headerSizeBytes());
        print("SERVER ACCEPTED NEW CLIENT ");
        client.register(selector, SelectionKey.OP_READ, clientCtx);
    }

    private void closeChannel(SelectionKey key) {
        if (key == null) return;
        SocketChannel channel = (SocketChannel) key.channel();
        print("SERVER CLOSING CHANNEL");
        try {
            key.cancel();
            channel.close();
        } catch (IOException e) {
            log.warning("Error closing channel " + e);
        }
    }

    static final boolean logEnabled = false;

    public static void print(Object any) {
        if (logEnabled) System.out.print(any);
    }
}
