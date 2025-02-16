package se.edinjakupovic;

import java.nio.ByteBuffer;

public interface MessageHandler {
    ByteBuffer processMessage(ByteBuffer byteBuffer);
}
