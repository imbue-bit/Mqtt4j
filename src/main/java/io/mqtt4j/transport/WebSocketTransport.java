package io.mqtt4j.transport;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT-over-WebSocket transport using the JDK built-in {@link java.net.http.WebSocket}.
 *
 * <p>MQTT over WebSocket transmits MQTT packets as WebSocket binary frames
 * with the subprotocol {@code "mqtt"}. This transport bridges the WebSocket
 * API to the {@link MqttTransport} stream-based interface:</p>
 *
 * <ul>
 *   <li>{@link #getInputStream()} returns a {@link PipedInputStream} fed by
 *       the WebSocket listener receiving binary frames</li>
 *   <li>{@link #getOutputStream()} returns a custom {@link OutputStream} that
 *       sends data as WebSocket binary frames</li>
 * </ul>
 *
 * <p>Supports both {@code ws://} (plain) and {@code wss://} (TLS) connections.
 * An optional {@link SSLContext} can be provided for custom TLS configuration.</p>
 *
 * @see MqttTransport
 */
public class WebSocketTransport implements MqttTransport {

    private static final Logger LOG = Logger.getLogger(WebSocketTransport.class.getName());

    /** Default pipe buffer size: 128 KB. */
    private static final int PIPE_BUFFER_SIZE = 128 * 1024;

    /** MQTT WebSocket subprotocol. */
    private static final String MQTT_SUBPROTOCOL = "mqtt";

    private final boolean useSsl;
    private final SSLContext sslContext;
    private final String path;

    private volatile WebSocket webSocket;
    private volatile HttpClient httpClient;
    private volatile boolean connected;

    private PipedInputStream pipedIn;
    private PipedOutputStream pipedOut;
    private WebSocketOutputStream wsOutputStream;
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * Creates a WebSocket transport.
     *
     * @param useSsl     {@code true} to use {@code wss://}, {@code false} for {@code ws://}
     * @param sslContext optional SSL context for TLS (only used when {@code useSsl} is true;
     *                   {@code null} for the JVM default)
     * @param path       optional WebSocket path (defaults to {@code "/mqtt"} if {@code null})
     */
    public WebSocketTransport(boolean useSsl, SSLContext sslContext, String path) {
        this.useSsl = useSsl;
        this.sslContext = sslContext;
        this.path = (path != null && !path.isEmpty()) ? path : "/mqtt";
    }

    /**
     * Creates a plain WebSocket transport with the default path {@code "/mqtt"}.
     */
    public WebSocketTransport() {
        this(false, null, "/mqtt");
    }

    /**
     * Creates a WebSocket transport with the specified TLS flag.
     *
     * @param useSsl {@code true} for wss://, {@code false} for ws://
     */
    public WebSocketTransport(boolean useSsl) {
        this(useSsl, null, "/mqtt");
    }

    /**
     * Establishes a WebSocket connection to the specified host and port.
     *
     * <p>Builds the WebSocket URI (ws:// or wss://), creates an {@link HttpClient},
     * and performs the WebSocket handshake with the {@code "mqtt"} subprotocol.</p>
     *
     * @param host      the broker hostname or IP address
     * @param port      the broker port
     * @param timeoutMs connection timeout in milliseconds
     * @throws IOException if the connection fails
     */
    @Override
    public void connect(String host, int port, int timeoutMs) throws IOException {
        Objects.requireNonNull(host, "host must not be null");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }

        String scheme = useSsl ? "wss" : "ws";
        URI uri = URI.create(scheme + "://" + host + ":" + port + path);
        LOG.log(Level.FINE, "WebSocket connecting to {0} (timeout={1}ms)",
                new Object[]{uri, timeoutMs});

        // Set up the piped stream pair
        pipedOut = new PipedOutputStream();
        pipedIn = new PipedInputStream(pipedOut, PIPE_BUFFER_SIZE);

        // Build HTTP client
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs));
        if (useSsl && sslContext != null) {
            clientBuilder.sslContext(sslContext);
        }
        httpClient = clientBuilder.build();

        // Create WebSocket listener
        MqttWebSocketListener listener = new MqttWebSocketListener(pipedOut);

        try {
            webSocket = httpClient.newWebSocketBuilder()
                    .subprotocols(MQTT_SUBPROTOCOL)
                    .connectTimeout(Duration.ofMillis(timeoutMs))
                    .buildAsync(uri, listener)
                    .get(timeoutMs, TimeUnit.MILLISECONDS);

            wsOutputStream = new WebSocketOutputStream(webSocket, writeLock);
            connected = true;
            LOG.log(Level.INFO, "WebSocket connected to {0}", uri);

        } catch (TimeoutException e) {
            closeQuietly();
            throw new IOException("WebSocket connection timed out: " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeQuietly();
            throw new IOException("WebSocket connection interrupted: " + uri, e);
        } catch (ExecutionException e) {
            closeQuietly();
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioe) {
                throw ioe;
            }
            throw new IOException("WebSocket connection failed: " + uri, cause);
        }
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (!connected || wsOutputStream == null) {
            throw new IOException("Transport is not connected");
        }
        return wsOutputStream;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (!connected || pipedIn == null) {
            throw new IOException("Transport is not connected");
        }
        return pipedIn;
    }

    @Override
    public boolean isConnected() {
        return connected && webSocket != null;
    }

    @Override
    public void close() throws IOException {
        connected = false;
        LOG.log(Level.FINE, "Closing WebSocket transport");

        WebSocket ws = this.webSocket;
        this.webSocket = null;

        if (ws != null) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "closing")
                        .get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.log(Level.FINE, "WebSocket close handshake failed (non-critical)", e);
            }
        }

        closeStream(wsOutputStream);
        closeStream(pipedIn);
        closeStream(pipedOut);

        wsOutputStream = null;
        pipedIn = null;
        pipedOut = null;

        // Shut down the HttpClient executor (Java 21+)
        HttpClient client = this.httpClient;
        this.httpClient = null;
        if (client != null) {
            client.close();
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }

    private static void closeStream(AutoCloseable stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
    }

    @Override
    public String toString() {
        if (connected) {
            String scheme = useSsl ? "wss" : "ws";
            return "WebSocketTransport[" + scheme + ", connected]";
        }
        return "WebSocketTransport[disconnected]";
    }

    // ── WebSocket Listener ─────────────────────────────────────────────────

    /**
     * WebSocket listener that receives binary frames and pipes them
     * into a {@link PipedOutputStream} for consumption by the MQTT decoder.
     *
     * <p>Handles fragmented binary messages by accumulating partial frames
     * until the final fragment is received.</p>
     */
    private static final class MqttWebSocketListener implements WebSocket.Listener {

        private final PipedOutputStream output;
        private ByteBuffer fragmentBuffer;

        MqttWebSocketListener(PipedOutputStream output) {
            this.output = output;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            LOG.log(Level.FINE, "WebSocket opened");
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            try {
                if (!last) {
                    // Fragment: accumulate into buffer
                    if (fragmentBuffer == null) {
                        fragmentBuffer = ByteBuffer.allocate(data.remaining() * 2);
                    }
                    if (fragmentBuffer.remaining() < data.remaining()) {
                        // Grow the fragment buffer
                        ByteBuffer grown = ByteBuffer.allocate(
                                (fragmentBuffer.position() + data.remaining()) * 2);
                        fragmentBuffer.flip();
                        grown.put(fragmentBuffer);
                        fragmentBuffer = grown;
                    }
                    fragmentBuffer.put(data);
                } else {
                    // Final fragment (or complete message)
                    byte[] bytes;
                    if (fragmentBuffer != null) {
                        // Append final fragment and extract
                        if (fragmentBuffer.remaining() < data.remaining()) {
                            ByteBuffer grown = ByteBuffer.allocate(
                                    fragmentBuffer.position() + data.remaining());
                            fragmentBuffer.flip();
                            grown.put(fragmentBuffer);
                            fragmentBuffer = grown;
                        }
                        fragmentBuffer.put(data);
                        fragmentBuffer.flip();
                        bytes = new byte[fragmentBuffer.remaining()];
                        fragmentBuffer.get(bytes);
                        fragmentBuffer = null;
                    } else {
                        bytes = new byte[data.remaining()];
                        data.get(bytes);
                    }
                    output.write(bytes);
                    output.flush();
                }
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Error writing to pipe from WebSocket", e);
            }

            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            LOG.log(Level.FINE, "WebSocket closed: code={0}, reason={1}",
                    new Object[]{statusCode, reason});
            try {
                output.close();
            } catch (IOException ignored) {
                // best-effort
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            LOG.log(Level.WARNING, "WebSocket error", error);
            try {
                output.close();
            } catch (IOException ignored) {
                // best-effort
            }
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            // MQTT over WebSocket should only use binary frames; ignore text
            LOG.log(Level.WARNING, "Received unexpected text frame on MQTT WebSocket");
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            webSocket.request(1);
            return null;
        }
    }

    // ── WebSocket OutputStream ─────────────────────────────────────────────

    /**
     * Custom {@link OutputStream} that sends data as WebSocket binary frames.
     *
     * <p>Uses a {@link ReentrantLock} for thread safety to avoid virtual thread
     * pinning that would occur with {@code synchronized}.</p>
     */
    private static final class WebSocketOutputStream extends OutputStream {

        private final WebSocket webSocket;
        private final ReentrantLock lock;

        WebSocketOutputStream(WebSocket webSocket, ReentrantLock lock) {
            this.webSocket = webSocket;
            this.lock = lock;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            Objects.requireNonNull(b, "buffer must not be null");
            if (off < 0 || len < 0 || off + len > b.length) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) return;

            ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
            lock.lock();
            try {
                CompletableFuture<WebSocket> future = webSocket.sendBinary(buffer, true)
                        .toCompletableFuture();
                future.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("WebSocket write interrupted", e);
            } catch (ExecutionException e) {
                throw new IOException("WebSocket write failed", e.getCause());
            } catch (TimeoutException e) {
                throw new IOException("WebSocket write timed out", e);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void flush() {
            // WebSocket binary frames are sent immediately
        }

        @Override
        public void close() {
            // Closing is handled by WebSocketTransport.close()
        }
    }
}
