package io.mqtt4j.codec;

/**
 * MQTT PUBACK Control Packet.
 *
 * <p>Sent as the QoS 1 acknowledgement for a PUBLISH packet. In v3.1.1 the
 * variable header contains only the packet identifier. In v5.0 a reason code
 * and properties are also present.</p>
 *
 * @param packetId   the packet identifier being acknowledged
 * @param reasonCode the v5.0 reason code ({@code null} for v3.1.1)
 * @param properties the v5.0 properties ({@code null} for v3.1.1)
 */
public record PubAckPacket(
    int packetId,
    MqttReasonCode reasonCode,
    MqttProperties properties
) implements MqttPacket {

    /** Creates a v3.1.1 PUBACK (no reason code or properties). */
    public PubAckPacket(int packetId) {
        this(packetId, null, null);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.PUBACK;
    }
}
