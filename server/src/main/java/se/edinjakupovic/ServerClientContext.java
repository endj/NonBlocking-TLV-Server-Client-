package se.edinjakupovic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static se.edinjakupovic.ServerConstants.TLV_TYPE_MASK;
import static se.edinjakupovic.utils.PayloadUtils.isKeepAlive;

public class ServerClientContext {
    public ClientStatus status;

    public final ByteBuffer headerBuffer;
    public byte tlvType = -1;
    public int requestLength = -1;

    public ByteBuffer bodyBuffer;
    public ByteBuffer responseBuffer;
    public int responseLength;

    public boolean keepAlive;

    public ServerClientContext(int headerSize) {
        headerBuffer = ByteBuffer.allocate(headerSize);
        status = ClientStatus.READING_HEADER;
    }

    public void setResponse(ByteBuffer response) {
        this.responseBuffer = response;
        status = ClientStatus.WRITING_RESPONSE;
        responseLength = responseBuffer.remaining();
    }

    public int readHeader(SocketChannel clientChannel,
                          SelectionKey key) throws IOException {
        int read = clientChannel.read(headerBuffer);
        if (read < 0) {
            clientChannel.close();
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
        byte typeByte = headerBuffer.get();

        tlvType = (byte) (typeByte & TLV_TYPE_MASK);
        keepAlive = isKeepAlive(typeByte);

        requestLength = headerBuffer.getInt();
        if (bodyBuffer == null || bodyBuffer.capacity() < requestLength) {
            bodyBuffer = ByteBuffer.allocate(requestLength);
        }
        status = ClientStatus.READING_BODY;
    }

    public void resetCtx() {
        headerBuffer.clear();
        tlvType = -1;
        requestLength = -1;
        bodyBuffer.clear();
        responseBuffer = null;
        responseLength = 0;
        status = ClientStatus.READING_HEADER;
        keepAlive = false;
    }
}
