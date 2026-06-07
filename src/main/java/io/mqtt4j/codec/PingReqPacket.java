package io.mqtt4j.codec;

/**
 * MQTT PINGREQ Control Packet.
 *
 * <p>Sent by the client to the server to indicate it is alive and to check
 * that the server is still responding. Contains no variable header or payload.
 * Wire format: {@code 0xC0 0x00}.</p>
 */
public record PingReqPacket() implements MqttPacket {

    /** Shared singleton instance (the packet carries no state). */
    public static final PingReqPacket INSTANCE = new PingReqPacket();

    @Override
    public MqttPacketType type() {
        return MqttPacketType.PINGREQ;
    }
}
