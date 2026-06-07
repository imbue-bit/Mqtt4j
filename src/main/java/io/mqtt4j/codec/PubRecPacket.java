package io.mqtt4j.codec;

/**
 * MQTT PUBREC Control Packet.
 *
 * <p>Sent as the first acknowledgement in the QoS 2 delivery protocol.
 * Structure is identical to PUBACK.</p>
 *
 * @param packetId   the packet identifier being acknowledged
 * @param reasonCode the v5.0 reason code ({@code null} for v3.1.1)
 * @param properties the v5.0 properties ({@code null} for v3.1.1)
 */
public record PubRecPacket(
    int packetId,
    MqttReasonCode reasonCode,
    MqttProperties properties
) implements MqttPacket {

    /** Creates a v3.1.1 PUBREC (no reason code or properties). */
    public PubRecPacket(int packetId) {
        this(packetId, null, null);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.PUBREC;
    }
}
