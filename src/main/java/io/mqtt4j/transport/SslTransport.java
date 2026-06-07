package io.mqtt4j.transport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SSL/TLS transport implementation using {@link SSLSocket}.
 *
 * <p>Extends {@link TcpTransport} to layer TLS on top of the TCP connection.
 * An optional {@link SSLContext} can be supplied for custom trust managers,
 * key managers, or protocol selection. If {@code null} is provided, the
 * JVM's default SSL context is used.</p>
 *
 * <p>After connecting, a TLS handshake is performed before the transport
 * is considered ready for use.</p>
 *
 * @see TcpTransport
 * @see MqttTransport
 */
public class SslTransport implements MqttTransport {

    private static final Logger LOG = Logger.getLogger(SslTransport.class.getName());

    private final SSLSocketFactory sslSocketFactory;
    private final int soTimeoutMs;
    private volatile SSLSocket sslSocket;

    /**
     * Creates an SSL transport with the given SSL context and socket timeout.
     *
     * @param sslContext  the SSL context to use, or {@code null} for the JVM default
     * @param soTimeoutMs socket read timeout in milliseconds (0 = infinite)
     */
    public SslTransport(SSLContext sslContext, int soTimeoutMs) {
        this.soTimeoutMs = soTimeoutMs;
        if (sslContext != null) {
            this.sslSocketFactory = sslContext.getSocketFactory();
        } else {
            try {
                this.sslSocketFactory = SSLContext.getDefault().getSocketFactory();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Default SSLContext not available", e);
            }
        }
    }

    /**
     * Creates an SSL transport with the JVM default SSL context and no read timeout.
     */
    public SslTransport() {
        this(null, 0);
    }

    /**
     * Creates an SSL transport with the given SSL context and no read timeout.
     *
     * @param sslContext the SSL context to use, or {@code null} for the JVM default
     */
    public SslTransport(SSLContext sslContext) {
        this(sslContext, 0);
    }

    /**
     * Establishes a TLS connection to the specified host and port.
     *
     * <p>Creates an unconnected SSL socket, connects with the given timeout,
     * then performs the TLS handshake.</p>
     *
     * @param host      the broker hostname or IP address
     * @param port      the broker port (typically 8883)
     * @param timeoutMs connection timeout in milliseconds
     * @throws IOException if the connection or TLS handshake fails
     */
    @Override
    public void connect(String host, int port, int timeoutMs) throws IOException {
        Objects.requireNonNull(host, "host must not be null");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }

        LOG.log(Level.FINE, "SSL connecting to {0}:{1} (timeout={2}ms)",
                new Object[]{host, port, timeoutMs});

        // Create an unconnected SSLSocket by wrapping a plain socket
        // This allows us to set the connection timeout before connecting
        Socket plainSocket = new Socket();
        plainSocket.setTcpNoDelay(true);
        plainSocket.setSoTimeout(soTimeoutMs);
        plainSocket.connect(new InetSocketAddress(host, port), timeoutMs);

        SSLSocket ssl = (SSLSocket) sslSocketFactory.createSocket(
                plainSocket, host, port, /* autoClose */ true);

        // Enable SNI by setting endpoint identification
        ssl.getSSLParameters().setEndpointIdentificationAlgorithm("HTTPS");

        LOG.log(Level.FINE, "Starting TLS handshake with {0}:{1}", new Object[]{host, port});
        ssl.startHandshake();

        this.sslSocket = ssl;
        LOG.log(Level.INFO, "SSL connected to {0}:{1} (protocol={2}, cipher={3})",
                new Object[]{host, port,
                        ssl.getSession().getProtocol(),
                        ssl.getSession().getCipherSuite()});
    }

    @Override
    public java.io.OutputStream getOutputStream() throws IOException {
        SSLSocket s = this.sslSocket;
        if (s == null || s.isClosed()) {
            throw new IOException("Transport is not connected");
        }
        return s.getOutputStream();
    }

    @Override
    public java.io.InputStream getInputStream() throws IOException {
        SSLSocket s = this.sslSocket;
        if (s == null || s.isClosed()) {
            throw new IOException("Transport is not connected");
        }
        return s.getInputStream();
    }

    @Override
    public boolean isConnected() {
        SSLSocket s = this.sslSocket;
        return s != null && !s.isClosed() && s.isConnected();
    }

    /**
     * Closes the SSL socket, releasing all associated resources.
     *
     * <p>This method is safe to call multiple times and will not throw
     * if the socket is already closed or was never connected.</p>
     */
    @Override
    public void close() throws IOException {
        SSLSocket s = this.sslSocket;
        this.sslSocket = null;
        if (s != null) {
            LOG.log(Level.FINE, "Closing SSL transport");
            try {
                s.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Error closing SSL socket", e);
                throw e;
            }
        }
    }

    @Override
    public String toString() {
        SSLSocket s = this.sslSocket;
        if (s != null && s.isConnected()) {
            return "SslTransport[" + s.getRemoteSocketAddress() + "]";
        }
        return "SslTransport[disconnected]";
    }
}
