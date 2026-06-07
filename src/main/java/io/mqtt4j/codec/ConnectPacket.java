package io.mqtt4j.codec;

import io.mqtt4j.MqttVersion;
import io.mqtt4j.message.MqttQoS;

import java.util.Objects;

/**
 * MQTT CONNECT Control Packet.
 *
 * <p>Sent by the client to the server as the first packet after establishing
 * the network connection. The variable header contains the protocol name,
 * protocol level, connect flags, and keep alive. The payload contains the
 * Client Identifier and optionally the Will, Username, and Password.</p>
 *
 * <p>In MQTT v5.0, properties may be present in both the variable header
 * (connection properties) and in the will portion of the payload (will
 * properties).</p>
 *
 * @param version        the MQTT protocol version
 * @param cleanSession   clean session (v3.1.1) / clean start (v5.0) flag
 * @param keepAliveSeconds the keep-alive interval in seconds
 * @param clientId       the client identifier (non-null, may be empty)
 * @param willTopic      the will topic ({@code null} if no will)
 * @param willMessage    the will message payload ({@code null} if no will)
 * @param willQos        the will QoS level ({@code null} defaults to AT_MOST_ONCE)
 * @param willRetain     the will retain flag
 * @param username       the username ({@code null} if not present)
 * @param password       the password ({@code null} if not present)
 * @param properties     the v5.0 connection properties ({@code null} for v3.1.1)
 * @param willProperties the v5.0 will properties ({@code null} for v3.1.1 or no will)
 */
public record ConnectPacket(
    MqttVersion version,
    boolean cleanSession,
    int keepAliveSeconds,
    String clientId,
    String willTopic,
    byte[] willMessage,
    MqttQoS willQos,
    boolean willRetain,
    String username,
    byte[] password,
    MqttProperties properties,
    MqttProperties willProperties
) implements MqttPacket {

    public ConnectPacket {
        Objects.requireNonNull(version, "version must not be null");
        Objects.requireNonNull(clientId, "clientId must not be null");
        if (willQos == null) {
            willQos = MqttQoS.AT_MOST_ONCE;
        }
        if (keepAliveSeconds < 0 || keepAliveSeconds > 65535) {
            throw new IllegalArgumentException("keepAliveSeconds out of range [0, 65535]: " + keepAliveSeconds);
        }
    }

    /**
     * Returns {@code true} if a will message is configured.
     */
    public boolean hasWill() {
        return willTopic != null;
    }

    /**
     * Returns {@code true} if a username is present.
     */
    public boolean hasUsername() {
        return username != null;
    }

    /**
     * Returns {@code true} if a password is present.
     */
    public boolean hasPassword() {
        return password != null;
    }

    /**
     * Encodes the connect flags byte.
     *
     * <pre>
     * Bit 7: Username Flag
     * Bit 6: Password Flag
     * Bit 5: Will Retain
     * Bit 4-3: Will QoS
     * Bit 2: Will Flag
     * Bit 1: Clean Session / Clean Start
     * Bit 0: Reserved (0)
     * </pre>
     */
    public int connectFlagsByte() {
        int flags = 0;
        if (hasUsername())    flags |= 0x80;
        if (hasPassword())   flags |= 0x40;
        if (hasWill()) {
            if (willRetain)  flags |= 0x20;
            flags |= (willQos.value() & 0x03) << 3;
            flags |= 0x04;
        }
        if (cleanSession)    flags |= 0x02;
        return flags;
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.CONNECT;
    }

    // ── Builder for convenient construction ────────────────────────────────

    /**
     * Creates a minimal v3.1.1 CONNECT packet builder.
     */
    public static Builder builder(MqttVersion version, String clientId) {
        return new Builder(version, clientId);
    }

    /**
     * Fluent builder for {@link ConnectPacket}.
     */
    public static final class Builder {
        private final MqttVersion version;
        private final String clientId;
        private boolean cleanSession = true;
        private int keepAliveSeconds = 60;
        private String willTopic;
        private byte[] willMessage;
        private MqttQoS willQos = MqttQoS.AT_MOST_ONCE;
        private boolean willRetain;
        private String username;
        private byte[] password;
        private MqttProperties properties;
        private MqttProperties willProperties;

        private Builder(MqttVersion version, String clientId) {
            this.version = version;
            this.clientId = clientId;
        }

        public Builder cleanSession(boolean cleanSession) { this.cleanSession = cleanSession; return this; }
        public Builder keepAlive(int seconds) { this.keepAliveSeconds = seconds; return this; }
        public Builder will(String topic, byte[] message, MqttQoS qos, boolean retain) {
            this.willTopic = topic; this.willMessage = message;
            this.willQos = qos; this.willRetain = retain; return this;
        }
        public Builder username(String username) { this.username = username; return this; }
        public Builder password(byte[] password) { this.password = password; return this; }
        public Builder properties(MqttProperties properties) { this.properties = properties; return this; }
        public Builder willProperties(MqttProperties willProperties) { this.willProperties = willProperties; return this; }

        public ConnectPacket build() {
            return new ConnectPacket(version, cleanSession, keepAliveSeconds,
                clientId, willTopic, willMessage, willQos, willRetain,
                username, password, properties, willProperties);
        }
    }
}
