package se.edinjakupovic;

import org.junit.jupiter.api.Test;
import se.edinjakupovic.utils.IterativeByteClient;
import se.edinjakupovic.utils.TestServer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static se.edinjakupovic.utils.PayloadUtils.payload;

class ServerTest {

    @Test
    void shouldCloseConnectionOnInvalidType() {
        try (var server = TestServer.withHandler((byte) 1, _ -> ByteBuffer.allocate(0))) {
            ByteBuffer response = server.testClient().sendPayload(payload((byte) 69, "Goodbye World"));
            byte responseType = response.get();
            assertThat(responseType).isEqualTo(ServerConstants.ERROR_TYPE);
        }
    }

    @Test
    void shouldSupportMultipleHandlers() {
        try (var server = TestServer.withHandlers(Map.of(
                (byte) 0, _ -> ByteBuffer.wrap("Hi there 0".getBytes(StandardCharsets.UTF_8)),
                (byte) 1, _ -> ByteBuffer.wrap("Hi there 1".getBytes(StandardCharsets.UTF_8))
        ))) {
            SimpleClient client = server.testClient();

            ByteBuffer response = client.sendPayload(payload((byte) 0, "Hello World"));
            assertThat(utf8String(response)).isEqualTo("Hi there 0");

            response = client.sendPayload(payload((byte) 1, "Another message"));
            assertThat(utf8String(response)).isEqualTo("Hi there 1");
        }
    }

    @Test
    void multipleRequests() {
        try (var server = TestServer.withHandler((byte) 1, _ -> ByteBuffer.wrap(new byte[]{42}))) {
            for (int i = 0; i < 5; i++) {
                ByteBuffer response = server.testClient().sendPayload(payload((byte) 1, "Goodbye World"));
                assertThat(response.get()).isEqualTo((byte) 42);
            }
        }
    }

    @Test
    void concurrentConnections() {
        int bodyLength = "Hi".getBytes(StandardCharsets.UTF_8).length;
        var payloadLength = 5 + bodyLength;
        var clients = List.of(
                new IterativeByteClient(new InetSocketAddress(8080), payload((byte) 1, "Hi")),
                new IterativeByteClient(new InetSocketAddress(8080), payload((byte) 1, "Hi")),
                new IterativeByteClient(new InetSocketAddress(8080), payload((byte) 1, "Hi")),
                new IterativeByteClient(new InetSocketAddress(8080), payload((byte) 1, "Hi"))
        );
        try (var server = TestServer.withHandler((byte) 1, _ -> ByteBuffer.wrap(new byte[]{42, 41, 40}))) {

            clients.forEach(c -> assertThat(c.connectionClosed).isTrue());
            clients.forEach(IterativeByteClient::openConnection);
            clients.forEach(c -> assertThat(c.connectionClosed).isFalse());

            clients.forEach(c -> assertThat(c.bytesSent).isZero());
            clients.forEach(c -> assertThat(c.bytesRead).isZero());

            clients.forEach(c -> c.writeXBytes(2));
            clients.forEach(c -> assertThat(c.bytesSent).isEqualTo(2));
            clients.forEach(c -> assertThat(c.bytesRead).isZero());

            clients.forEach(c -> c.writeXBytes(3));
            clients.forEach(c -> assertThat(c.bytesSent).isEqualTo(5));
            clients.forEach(c -> assertThat(c.bytesRead).isZero());

            clients.forEach(c -> c.writeXBytes(bodyLength));
            clients.forEach(c -> assertThat(c.bytesSent).isEqualTo(payloadLength));

            clients.forEach(c -> c.tryReadXBytes(1));
            clients.forEach(c -> assertThat(c.bytesRead).isEqualTo(1));

            clients.forEach(c -> c.tryReadXBytes(2));
            clients.forEach(c -> assertThat(c.bytesRead).isEqualTo(3));
            clients.forEach(c -> assertThat(c.connectionClosed).isFalse());

            clients.forEach(c -> c.tryReadXBytes(1));
            clients.forEach(c -> assertThat(c.connectionClosed).isTrue());
            clients.forEach(c -> assertThat(c.bytesRead).isEqualTo(3));

            clients.forEach(c -> {
                int read = c.readBuffer.position();
                assertThat(read).isEqualTo(3);
                c.readBuffer.flip();
                byte a = c.readBuffer.get();
                assertThat(a).isEqualTo((byte) 42);
                byte b = c.readBuffer.get();
                assertThat(b).isEqualTo((byte)41);
                byte d = c.readBuffer.get();
                assertThat(d).isEqualTo((byte)40);
            });

        }
    }

    private String utf8String(ByteBuffer response) {
        return new String(response.array(), 0, response.limit());
    }

}