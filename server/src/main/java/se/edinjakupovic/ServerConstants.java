package se.edinjakupovic;

import java.nio.ByteBuffer;

import static java.lang.Byte.MAX_VALUE;

public class ServerConstants {
    public static Byte ERROR_TYPE = MAX_VALUE;
    public static ByteBuffer ERROR_TYPE_BASE = ByteBuffer.wrap(new byte[]{MAX_VALUE, 0, 0, 0, 0});
    public static final byte KEEP_ALIVE_BIT = (byte) 0x80;
    public static final byte TLV_TYPE_MASK = (byte) 0x7f;
}
