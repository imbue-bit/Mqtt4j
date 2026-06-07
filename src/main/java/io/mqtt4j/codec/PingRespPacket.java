package io.mqtt4j.codec;

/**
 * MQTT PINGRESP Control Packet.
 *
 * <p>Sent by the server in response to a PINGREQ. Contains no variable header
 * or payload. Wire format: {@code 0xD0 0x00}.</p>
 */
public record PingRespPacket() implements MqttPacket {

    /** Shared singleton instance (the packet carries no state). */
    public static final PingRespPacket INSTANCE = new PingRespPacket();

    @Override
    public MqttPacketType type() {
        return MqttPacketType.PINGRESP;
    }
}
