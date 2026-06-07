package io.mqtt4j.codec;

import java.util.List;
import java.util.Objects;

/**
 * MQTT SUBACK Control Packet.
 *
 * <p>Sent by the server to acknowledge a SUBSCRIBE request. Contains a list
 * of reason codes corresponding to each subscription in the original
 * SUBSCRIBE packet.</p>
 *
 * <p>Reason code values:</p>
 * <ul>
 *   <li>{@code 0x00} – Success, maximum QoS 0</li>
 *   <li>{@code 0x01} – Success, maximum QoS 1</li>
 *   <li>{@code 0x02} – Success, maximum QoS 2</li>
 *   <li>{@code 0x80} – Failure</li>
 * </ul>
 *
 * @param packetId    the packet identifier matching the SUBSCRIBE
 * @param reasonCodes the per-topic-filter reason codes
 * @param properties  the v5.0 properties ({@code null} for v3.1.1)
 */
public record SubAckPacket(
    int packetId,
    List<Integer> reasonCodes,
    MqttProperties properties
) implements MqttPacket {

    public SubAckPacket {
        Objects.requireNonNull(reasonCodes, "reasonCodes must not be null");
        reasonCodes = List.copyOf(reasonCodes);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.SUBACK;
    }
}
