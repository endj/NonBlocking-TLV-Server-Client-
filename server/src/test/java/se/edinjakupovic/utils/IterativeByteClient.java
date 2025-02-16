package se.edinjakupovic.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class IterativeByteClient {

    private final InetSocketAddress address;
    public int bytesSent = 0;
    public int bytesRead = 0;
    private final ByteBuffer payload;
    public  ByteBuffer readBuffer = ByteBuffer.allocate(64);
    SocketChannel channel;
    public boolean connectionClosed = true;

    public IterativeByteClient(InetSocketAddress address,
                               ByteBuffer payload) {
        this.address = address;
        this.payload = payload;
    }

    public IterativeByteClient openConnection() {
        try {
            channel = SocketChannel.open(address);
            connectionClosed = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void closeConnection() {
        if (channel == null) throw new RuntimeException("Connection not opened");
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int tryReadXBytes(int x) {
        if(channel == null) throw new RuntimeException("Connection not opened");
        if(connectionClosed) return 0;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(x);
            int readLocal = 0;
            int attempts = 1000;
            while (attempts-->0 && (readLocal = channel.read(buffer)) == 0) {
            }
            if(readLocal == -1) {
                connectionClosed = true;
                return 0;
            }
             readBuffer.put(buffer.array());
            bytesRead += readLocal;
            return readLocal;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int writeXBytes(int x) {
        if (channel == null) throw new RuntimeException("Connection not opened");
        try {
            var bs =  channel.write(payload.slice(bytesSent, x));
            bytesSent += bs;
            return bs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
