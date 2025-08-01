package se.edinjakupovic;

import java.net.InetSocketAddress;
import java.util.Map;

public record ServerConfig(
        InetSocketAddress bindAddress,
        TLVConfig config,
        long requestTimeoutMillis,
        long responseTimeoutMillis,
        int connectionBacklog,
        int maxConnections,
        int workers,
        Map<Byte, MessageHandler> handlers,
        MessageHandler errorHandler
) {
}
