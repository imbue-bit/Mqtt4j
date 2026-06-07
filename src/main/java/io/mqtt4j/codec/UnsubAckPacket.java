package io.mqtt4j.codec;

import java.util.List;

/**
 * MQTT UNSUBACK Control Packet.
 *
 * <p>Sent by the server to acknowledge an UNSUBSCRIBE request. In v3.1.1
 * the packet contains only the packet identifier (no reason codes). In v5.0
 * a list of per-topic-filter reason codes and optional properties are present.</p>
 *
 * @param packetId    the packet identifier matching the UNSUBSCRIBE
 * @param reasonCodes the per-topic-filter reason codes (empty or {@code null} for v3.1.1)
 * @param properties  the v5.0 properties ({@code null} for v3.1.1)
 */
public record UnsubAckPacket(
    int packetId,
    List<Integer> reasonCodes,
    MqttProperties properties
) implements MqttPacket {

    public UnsubAckPacket {
        if (reasonCodes != null) {
            reasonCodes = List.copyOf(reasonCodes);
        }
    }

    /** Creates a v3.1.1 UNSUBACK (no reason codes or properties). */
    public UnsubAckPacket(int packetId) {
        this(packetId, null, null);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.UNSUBACK;
    }
}
