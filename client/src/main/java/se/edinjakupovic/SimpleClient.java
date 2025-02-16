package se.edinjakupovic;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SimpleClient {

    private final InetSocketAddress address;

    public SimpleClient(InetSocketAddress address) {
        this.address = address;
    }

    public ByteBuffer sendPayload(ByteBuffer payload) {
        try (SocketChannel socket = SocketChannel.open(address)) {
            socket.write(payload);
            ByteBuffer readBuffer = ByteBuffer.allocate(1024);

            int totalRead = 0;
            int bytesRead;
            while ((bytesRead = socket.read(readBuffer)) > 0) {
                totalRead += bytesRead;
            }

            readBuffer.flip();
            readBuffer.limit(totalRead);

            return readBuffer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
