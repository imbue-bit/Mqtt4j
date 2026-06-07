package io.mqtt4j.codec;

import java.util.Objects;

/**
 * MQTT AUTH Control Packet (MQTT v5.0 only).
 *
 * <p>Used for enhanced authentication exchanges between client and server.
 * This packet type does not exist in MQTT v3.1.1.</p>
 *
 * @param reasonCode the authentication reason code
 * @param properties the v5.0 properties (never {@code null})
 */
public record AuthPacket(
    MqttReasonCode reasonCode,
    MqttProperties properties
) implements MqttPacket {

    public AuthPacket {
        Objects.requireNonNull(reasonCode, "reasonCode must not be null");
        Objects.requireNonNull(properties, "properties must not be null");
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.AUTH;
    }
}
