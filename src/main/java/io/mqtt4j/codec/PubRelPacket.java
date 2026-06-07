package io.mqtt4j.codec;

/**
 * MQTT PUBREL Control Packet.
 *
 * <p>Sent as the second step in the QoS 2 delivery protocol. The fixed header
 * flags for PUBREL are reserved as {@code 0010}. Structure is identical to
 * PUBACK.</p>
 *
 * @param packetId   the packet identifier being acknowledged
 * @param reasonCode the v5.0 reason code ({@code null} for v3.1.1)
 * @param properties the v5.0 properties ({@code null} for v3.1.1)
 */
public record PubRelPacket(
    int packetId,
    MqttReasonCode reasonCode,
    MqttProperties properties
) implements MqttPacket {

    /** Creates a v3.1.1 PUBREL (no reason code or properties). */
    public PubRelPacket(int packetId) {
        this(packetId, null, null);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.PUBREL;
    }
}
