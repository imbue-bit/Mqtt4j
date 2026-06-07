package io.mqtt4j.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TCP transport implementation using {@link java.net.Socket}.
 *
 * <p>Uses blocking I/O which is efficient when paired with virtual threads.
 * A {@link java.util.concurrent.locks.ReentrantLock} is <em>not</em> needed
 * at the transport level — write synchronization is handled by the caller
 * (e.g. the session layer). The transport itself is a thin wrapper around
 * the raw socket.</p>
 *
 * <p>Socket options applied on connect:</p>
 * <ul>
 *   <li>{@code TCP_NODELAY = true} — disables Nagle's algorithm for low latency</li>
 *   <li>{@code SO_TIMEOUT} — configurable read timeout</li>
 * </ul>
 *
 * @see MqttTransport
 */
public class TcpTransport implements MqttTransport {

    private static final Logger LOG = Logger.getLogger(TcpTransport.class.getName());

    private final int soTimeoutMs;
    private volatile Socket socket;

    /**
     * Creates a TCP transport with the specified socket read timeout.
     *
     * @param soTimeoutMs socket read timeout in milliseconds (0 = infinite)
     */
    public TcpTransport(int soTimeoutMs) {
        this.soTimeoutMs = soTimeoutMs;
    }

    /**
     * Creates a TCP transport with no socket read timeout.
     */
    public TcpTransport() {
        this(0);
    }

    /**
     * Establishes a TCP connection to the specified host and port.
     *
     * <p>Creates a new {@link Socket}, sets {@code TCP_NODELAY} and
     * {@code SO_TIMEOUT}, then connects with the given timeout.</p>
     *
     * @param host      the broker hostname or IP address
     * @param port      the broker port (typically 1883)
     * @param timeoutMs connection timeout in milliseconds
     * @throws IOException if the connection fails
     */
    @Override
    public void connect(String host, int port, int timeoutMs) throws IOException {
        Objects.requireNonNull(host, "host must not be null");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }

        LOG.log(Level.FINE, "Connecting to {0}:{1} (timeout={2}ms)", new Object[]{host, port, timeoutMs});

        Socket s = createSocket();
        s.setTcpNoDelay(true);
        s.setSoTimeout(soTimeoutMs);
        s.connect(new InetSocketAddress(host, port), timeoutMs);

        this.socket = s;
        LOG.log(Level.INFO, "Connected to {0}:{1}", new Object[]{host, port});
    }

    /**
     * Creates the underlying socket. Subclasses (e.g. {@link SslTransport})
     * may override this to provide an SSL socket.
     *
     * @return a new, unconnected socket
     * @throws IOException if socket creation fails
     */
    protected Socket createSocket() throws IOException {
        return new Socket();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        Socket s = this.socket;
        if (s == null || s.isClosed()) {
            throw new IOException("Transport is not connected");
        }
        return s.getOutputStream();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        Socket s = this.socket;
        if (s == null || s.isClosed()) {
            throw new IOException("Transport is not connected");
        }
        return s.getInputStream();
    }

    @Override
    public boolean isConnected() {
        Socket s = this.socket;
        return s != null && !s.isClosed() && s.isConnected();
    }

    /**
     * Closes the TCP socket, releasing all associated resources.
     *
     * <p>This method is safe to call multiple times and will not throw
     * if the socket is already closed or was never connected.</p>
     */
    @Override
    public void close() throws IOException {
        Socket s = this.socket;
        this.socket = null;
        if (s != null) {
            LOG.log(Level.FINE, "Closing TCP transport");
            try {
                s.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Error closing socket", e);
                throw e;
            }
        }
    }

    @Override
    public String toString() {
        Socket s = this.socket;
        if (s != null && s.isConnected()) {
            return "TcpTransport[" + s.getRemoteSocketAddress() + "]";
        }
        return "TcpTransport[disconnected]";
    }
}
