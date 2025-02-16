package se.edinjakupovic.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static se.edinjakupovic.ServerConstants.KEEP_ALIVE_BIT;

public final class PayloadUtils {

    public static ByteBuffer payload(byte type, String body) {
        return payload(type, body, false);
    }

    public static ByteBuffer payload(byte type, String body, boolean keepAlive) {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        byte payloadLength = (byte) payload.length;

        byte typeByte = (byte) (type & 0x7F);
        if (keepAlive) {
            typeByte |= 0x80;
        }

        ByteBuffer buffer = ByteBuffer.allocate(5 + payloadLength);

        buffer.put(typeByte);
        buffer.put((byte) (payloadLength >> 24));
        buffer.put((byte) (payloadLength >> 16));
        buffer.put((byte) (payloadLength >> 8));
        buffer.put((byte) (payloadLength));
        buffer.put(payload);

        buffer.flip();
        return buffer;
    }

    public static boolean isKeepAlive(byte typeByte) {
        return (typeByte & KEEP_ALIVE_BIT) != 0;
    }
}
