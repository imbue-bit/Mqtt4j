package io.mqtt4j.codec;

/**
 * MQTT CONNACK Control Packet.
 *
 * <p>Sent by the server in response to a CONNECT packet. In v3.1.1 the
 * variable header contains only the session-present flag and a return code.
 * In v5.0 a reason code and properties are also present.</p>
 *
 * @param sessionPresent whether the server has a stored session for this client
 * @param returnCode     the v3.1.1 return code (0=accepted, 1-5=refused); in v5.0 this
 *                       mirrors the numeric value of the reason code
 * @param reasonCode     the v5.0 reason code ({@code null} for v3.1.1)
 * @param properties     the v5.0 properties ({@code null} for v3.1.1)
 */
public record ConnAckPacket(
    boolean sessionPresent,
    int returnCode,
    MqttReasonCode reasonCode,
    MqttProperties properties
) implements MqttPacket {

    /** Creates a v3.1.1 CONNACK with no properties. */
    public ConnAckPacket(boolean sessionPresent, int returnCode) {
        this(sessionPresent, returnCode, null, null);
    }

    /**
     * Returns {@code true} if the connection was accepted.
     */
    public boolean isAccepted() {
        return returnCode == 0;
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.CONNACK;
    }
}
