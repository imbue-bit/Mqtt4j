package io.mqtt4j.codec;

import java.util.List;
import java.util.Objects;

/**
 * MQTT UNSUBSCRIBE Control Packet.
 *
 * <p>Sent by the client to unsubscribe from one or more topic filters.
 * The fixed header flags are reserved as {@code 0010}.</p>
 *
 * @param packetId     the packet identifier
 * @param topicFilters the list of topic filters to unsubscribe from (non-empty)
 * @param properties   the v5.0 properties ({@code null} for v3.1.1)
 */
public record UnsubscribePacket(
    int packetId,
    List<String> topicFilters,
    MqttProperties properties
) implements MqttPacket {

    public UnsubscribePacket {
        Objects.requireNonNull(topicFilters, "topicFilters must not be null");
        if (topicFilters.isEmpty()) {
            throw new IllegalArgumentException("topicFilters must not be empty");
        }
        topicFilters = List.copyOf(topicFilters);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.UNSUBSCRIBE;
    }
}
