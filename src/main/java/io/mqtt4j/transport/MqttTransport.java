package io.mqtt4j.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Abstraction for MQTT transport layer.
 *
 * <p>Implementations provide the raw byte stream over which MQTT packets
 * are exchanged. Designed to be used with virtual threads — implementations
 * should use blocking I/O (which virtual threads handle efficiently).</p>
 *
 * @see TcpTransport
 * @see SslTransport
 * @see WebSocketTransport
 */
public interface MqttTransport extends AutoCloseable {

    /**
     * Establishes the transport connection.
     *
     * @param host      the broker hostname
     * @param port      the broker port
     * @param timeoutMs connection timeout in milliseconds
     * @throws IOException if the connection fails
     */
    void connect(String host, int port, int timeoutMs) throws IOException;

    /**
     * Returns the output stream for sending data.
     */
    OutputStream getOutputStream() throws IOException;

    /**
     * Returns the input stream for receiving data.
     */
    InputStream getInputStream() throws IOException;

    /**
     * Returns {@code true} if the transport is currently connected.
     */
    boolean isConnected();

    /**
     * Closes the transport connection.
     */
    @Override
    void close() throws IOException;
}
