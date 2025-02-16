package se.edinjakupovic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;

public class NioClientContext {
    Status status;
    private final CompletableFuture<Long> response;
    final ByteBuffer request;
    final ByteBuffer headerBuffer;
    ByteBuffer bodyBuffer;
    int requestLength;
    final boolean keepAlive;
    final int id;
    int tlvType;
    int responseLength;
    long start;

    public NioClientContext(ByteBuffer request,
                            CompletableFuture<Long> response,
                            boolean keepAlive,
                            int id) {
        this.request = request;
        this.requestLength = request.remaining();
        this.keepAlive = keepAlive;
        this.id = id;
        this.headerBuffer = ByteBuffer.allocate(5);
        this.response = response;
        this.status = Status.CONNECTING;
        this.start = System.currentTimeMillis();
    }

    public int readHeader(SocketChannel channel,
                          SelectionKey key) throws IOException {
        int read = channel.read(headerBuffer);
        if (read == -1) {
            channel.close();
            key.cancel();
            return -1;
        }
        if (read == 0) return 0;
        if (!headerBuffer.hasRemaining()) {
            flipToReadingBody();
        }
        return read;
    }

    private void flipToReadingBody() {
        headerBuffer.flip();
        tlvType = headerBuffer.get();
        responseLength = headerBuffer.getInt();
        bodyBuffer = ByteBuffer.allocate(responseLength);
        status = Status.READING_BODY;
    }

    public void onSuccess() {
        response.complete(elapsed());
    }

    public void onError(String reason) {
        response.completeExceptionally(new RuntimeException(reason));
    }

    enum Status {
        CONNECTING,
        WRITING,
        READING_HEADER,
        READING_BODY
    }

    public long elapsed() {
        return System.currentTimeMillis() - start;
    }
}
