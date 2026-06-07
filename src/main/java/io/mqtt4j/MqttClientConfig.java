package io.mqtt4j;

import io.mqtt4j.message.MqttQoS;
import io.mqtt4j.transport.MqttTransport;
import io.mqtt4j.transport.TcpTransport;
import io.mqtt4j.transport.SslTransport;
import io.mqtt4j.transport.WebSocketTransport;

import javax.net.ssl.SSLContext;
import java.util.Objects;
import java.util.UUID;

/**
 * Configuration for an {@link MqttClient} instance.
 *
 * <p>Use the {@link Builder} to construct a configuration with fluent API:</p>
 * <pre>{@code
 * MqttClientConfig config = MqttClientConfig.builder()
 *     .host("broker.emqx.io")
 *     .port(1883)
 *     .clientId("my-device-001")
 *     .version(MqttVersion.V3_1_1)
 *     .keepAlive(60)
 *     .cleanSession(true)
 *     .build();
 * }</pre>
 */
public final class MqttClientConfig {

    /** Transport protocol type. */
    public enum TransportType {
        /** Plain TCP socket. */
        TCP,
        /** TLS/SSL encrypted socket. */
        SSL,
        /** WebSocket (ws://). */
        WEBSOCKET,
        /** Secure WebSocket (wss://). */
        WEBSOCKET_SSL
    }

    // ── Connection ─────────────────────────────────────────────────────────
    private final String host;
    private final int port;
    private final TransportType transportType;
    private final SSLContext sslContext;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    // ── MQTT Protocol ──────────────────────────────────────────────────────
    private final MqttVersion version;
    private final String clientId;
    private final boolean cleanSession;
    private final int keepAliveSeconds;

    // ── Authentication ─────────────────────────────────────────────────────
    private final String username;
    private final byte[] password;

    // ── Last Will and Testament ────────────────────────────────────────────
    private final String willTopic;
    private final byte[] willMessage;
    private final MqttQoS willQos;
    private final boolean willRetain;

    // ── Auto-Reconnect ─────────────────────────────────────────────────────
    private final boolean autoReconnect;
    private final long reconnectInitialDelayMs;
    private final long reconnectMaxDelayMs;
    private final double reconnectMultiplier;
    private final int reconnectMaxRetries;

    // ── QoS / Flow Control ─────────────────────────────────────────────────
    private final int maxInflight;
    private final long publishTimeoutMs;
    private final int maxRetries;

    private MqttClientConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.transportType = builder.transportType;
        this.sslContext = builder.sslContext;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.socketTimeoutMs = builder.socketTimeoutMs;
        this.version = builder.version;
        this.clientId = builder.clientId;
        this.cleanSession = builder.cleanSession;
        this.keepAliveSeconds = builder.keepAliveSeconds;
        this.username = builder.username;
        this.password = builder.password;
        this.willTopic = builder.willTopic;
        this.willMessage = builder.willMessage;
        this.willQos = builder.willQos;
        this.willRetain = builder.willRetain;
        this.autoReconnect = builder.autoReconnect;
        this.reconnectInitialDelayMs = builder.reconnectInitialDelayMs;
        this.reconnectMaxDelayMs = builder.reconnectMaxDelayMs;
        this.reconnectMultiplier = builder.reconnectMultiplier;
        this.reconnectMaxRetries = builder.reconnectMaxRetries;
        this.maxInflight = builder.maxInflight;
        this.publishTimeoutMs = builder.publishTimeoutMs;
        this.maxRetries = builder.maxRetries;
    }

    // ── Getters ────────────────────────────────────────────────────────────

    public String host() { return host; }
    public int port() { return port; }
    public TransportType transportType() { return transportType; }
    public SSLContext sslContext() { return sslContext; }
    public int connectTimeoutMs() { return connectTimeoutMs; }
    public int socketTimeoutMs() { return socketTimeoutMs; }
    public MqttVersion version() { return version; }
    public String clientId() { return clientId; }
    public boolean cleanSession() { return cleanSession; }
    public int keepAliveSeconds() { return keepAliveSeconds; }
    public String username() { return username; }
    public byte[] password() { return password; }
    public String willTopic() { return willTopic; }
    public byte[] willMessage() { return willMessage; }
    public MqttQoS willQos() { return willQos; }
    public boolean willRetain() { return willRetain; }
    public boolean autoReconnect() { return autoReconnect; }
    public long reconnectInitialDelayMs() { return reconnectInitialDelayMs; }
    public long reconnectMaxDelayMs() { return reconnectMaxDelayMs; }
    public double reconnectMultiplier() { return reconnectMultiplier; }
    public int reconnectMaxRetries() { return reconnectMaxRetries; }
    public int maxInflight() { return maxInflight; }
    public long publishTimeoutMs() { return publishTimeoutMs; }
    public int maxRetries() { return maxRetries; }

    /** Creates a new transport instance based on the configured transport type. */
    public MqttTransport createTransport() {
        return switch (transportType) {
            case TCP -> new TcpTransport();
            case SSL -> new SslTransport(sslContext);
            case WEBSOCKET -> new WebSocketTransport(false, null, null);
            case WEBSOCKET_SSL -> new WebSocketTransport(true, sslContext, null);
        };
    }

    /** Returns a new Builder with default settings. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "MqttClientConfig{" +
               "host='" + host + '\'' +
               ", port=" + port +
               ", transport=" + transportType +
               ", version=" + version +
               ", clientId='" + clientId + '\'' +
               ", cleanSession=" + cleanSession +
               ", keepAlive=" + keepAliveSeconds +
               ", autoReconnect=" + autoReconnect +
               '}';
    }

    // ── Builder ────────────────────────────────────────────────────────────

    public static final class Builder {
        private String host = "localhost";
        private int port = 1883;
        private TransportType transportType = TransportType.TCP;
        private SSLContext sslContext = null;
        private int connectTimeoutMs = 10_000;
        private int socketTimeoutMs = 0;

        private MqttVersion version = MqttVersion.V3_1_1;
        private String clientId = "mqtt4j-" + UUID.randomUUID().toString().substring(0, 8);
        private boolean cleanSession = true;
        private int keepAliveSeconds = 60;

        private String username = null;
        private byte[] password = null;

        private String willTopic = null;
        private byte[] willMessage = null;
        private MqttQoS willQos = MqttQoS.AT_MOST_ONCE;
        private boolean willRetain = false;

        private boolean autoReconnect = true;
        private long reconnectInitialDelayMs = 1_000;
        private long reconnectMaxDelayMs = 30_000;
        private double reconnectMultiplier = 2.0;
        private int reconnectMaxRetries = -1; // infinite

        private int maxInflight = 65535;
        private long publishTimeoutMs = 30_000;
        private int maxRetries = 3;

        private Builder() {}

        // ── Connection ─────────────────────────────────────────────────

        /** Sets the broker hostname. Default: {@code "localhost"}. */
        public Builder host(String host) {
            this.host = Objects.requireNonNull(host);
            return this;
        }

        /** Sets the broker port. Default: {@code 1883}. */
        public Builder port(int port) {
            if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid port: " + port);
            this.port = port;
            return this;
        }

        /** Sets the transport type. Default: {@link TransportType#TCP}. */
        public Builder transportType(TransportType type) {
            this.transportType = Objects.requireNonNull(type);
            return this;
        }

        /** Convenience: use SSL transport on port 8883. */
        public Builder ssl() {
            this.transportType = TransportType.SSL;
            if (this.port == 1883) this.port = 8883;
            return this;
        }

        /** Convenience: use WebSocket transport. */
        public Builder webSocket() {
            this.transportType = TransportType.WEBSOCKET;
            return this;
        }

        /** Convenience: use secure WebSocket transport. */
        public Builder webSocketSsl() {
            this.transportType = TransportType.WEBSOCKET_SSL;
            return this;
        }

        /** Sets the SSL context for TLS connections. */
        public Builder sslContext(SSLContext ctx) {
            this.sslContext = ctx;
            return this;
        }

        /** Sets the connection timeout in milliseconds. Default: {@code 10000}. */
        public Builder connectTimeout(int ms) {
            this.connectTimeoutMs = ms;
            return this;
        }

        /** Sets the socket read timeout in milliseconds (0 = infinite). Default: {@code 0}. */
        public Builder socketTimeout(int ms) {
            this.socketTimeoutMs = ms;
            return this;
        }

        // ── MQTT Protocol ──────────────────────────────────────────────

        /** Sets the MQTT version. Default: {@link MqttVersion#V3_1_1}. */
        public Builder version(MqttVersion version) {
            this.version = Objects.requireNonNull(version);
            return this;
        }

        /** Convenience: use MQTT v5.0. */
        public Builder v5() {
            this.version = MqttVersion.V5_0;
            return this;
        }

        /** Sets the client identifier. Default: auto-generated UUID prefix. */
        public Builder clientId(String clientId) {
            this.clientId = Objects.requireNonNull(clientId);
            return this;
        }

        /** Sets the clean session flag. Default: {@code true}. */
        public Builder cleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        /** Sets the keep-alive interval in seconds. Default: {@code 60}. */
        public Builder keepAlive(int seconds) {
            if (seconds < 0 || seconds > 65535) throw new IllegalArgumentException("Invalid keepAlive: " + seconds);
            this.keepAliveSeconds = seconds;
            return this;
        }

        // ── Authentication ─────────────────────────────────────────────

        /** Sets the username for authentication. */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /** Sets the password for authentication. */
        public Builder password(byte[] password) {
            this.password = password;
            return this;
        }

        /** Sets the password as a UTF-8 string. */
        public Builder password(String password) {
            this.password = password != null ? password.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
            return this;
        }

        // ── Last Will ──────────────────────────────────────────────────

        /** Sets the Last Will and Testament message. */
        public Builder will(String topic, byte[] message, MqttQoS qos, boolean retain) {
            this.willTopic = topic;
            this.willMessage = message;
            this.willQos = qos;
            this.willRetain = retain;
            return this;
        }

        /** Sets a simple LWT with QoS 1 and no retain. */
        public Builder will(String topic, String message) {
            return will(topic, message.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                       MqttQoS.AT_LEAST_ONCE, false);
        }

        // ── Auto-Reconnect ─────────────────────────────────────────────

        /** Enables/disables auto-reconnect. Default: {@code true}. */
        public Builder autoReconnect(boolean enabled) {
            this.autoReconnect = enabled;
            return this;
        }

        /** Sets the initial reconnect delay in milliseconds. Default: {@code 1000}. */
        public Builder reconnectInitialDelay(long ms) {
            this.reconnectInitialDelayMs = ms;
            return this;
        }

        /** Sets the maximum reconnect delay in milliseconds. Default: {@code 30000}. */
        public Builder reconnectMaxDelay(long ms) {
            this.reconnectMaxDelayMs = ms;
            return this;
        }

        /** Sets the reconnect backoff multiplier. Default: {@code 2.0}. */
        public Builder reconnectMultiplier(double multiplier) {
            this.reconnectMultiplier = multiplier;
            return this;
        }

        /** Sets the max reconnect retries (-1 = infinite). Default: {@code -1}. */
        public Builder reconnectMaxRetries(int retries) {
            this.reconnectMaxRetries = retries;
            return this;
        }

        // ── QoS / Flow Control ─────────────────────────────────────────

        /** Sets the maximum in-flight messages. Default: {@code 65535}. */
        public Builder maxInflight(int max) {
            this.maxInflight = max;
            return this;
        }

        /** Sets the publish timeout in milliseconds. Default: {@code 30000}. */
        public Builder publishTimeout(long ms) {
            this.publishTimeoutMs = ms;
            return this;
        }

        /** Sets the max retry count for QoS 1/2. Default: {@code 3}. */
        public Builder maxRetries(int retries) {
            this.maxRetries = retries;
            return this;
        }

        /** Builds the configuration. */
        public MqttClientConfig build() {
            return new MqttClientConfig(this);
        }
    }
}
