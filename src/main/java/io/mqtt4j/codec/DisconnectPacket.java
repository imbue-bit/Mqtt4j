package io.mqtt4j.codec;

/**
 * MQTT DISCONNECT Control Packet.
 *
 * <p>In MQTT v3.1.1, this is a zero-length packet ({@code 0xE0 0x00}) sent
 * only by the client. In MQTT v5.0, both client and server may send it, and
 * it includes a reason code and optional properties.</p>
 *
 * @param reasonCode the v5.0 reason code ({@code null} for v3.1.1)
 * @param properties the v5.0 properties ({@code null} for v3.1.1)
 */
public record DisconnectPacket(
    MqttReasonCode reasonCode,
    MqttProperties properties
) implements MqttPacket {

    /** Convenience instance for v3.1.1 disconnects. */
    public static final DisconnectPacket V3_INSTANCE = new DisconnectPacket(null, null);

    @Override
    public MqttPacketType type() {
        return MqttPacketType.DISCONNECT;
    }
}
