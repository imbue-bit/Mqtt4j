package io.mqtt4j.codec;

import io.mqtt4j.message.MqttQoS;

import java.util.Objects;

/**
 * MQTT PUBLISH Control Packet.
 *
 * <p>Used to transport application messages. The fixed header flags encode
 * DUP, QoS, and RETAIN. The variable header contains the topic name and,
 * for QoS &gt; 0, a packet identifier. In v5.0, properties follow the
 * variable header.</p>
 *
 * @param dup        duplicate delivery flag
 * @param qos        quality-of-service level
 * @param retain     retain flag
 * @param topicName  the topic to publish to (non-null)
 * @param packetId   the packet identifier (0 for QoS 0)
 * @param payload    the application message payload (non-null, may be empty)
 * @param properties the v5.0 properties ({@code null} for v3.1.1)
 */
public record PublishPacket(
    boolean dup,
    MqttQoS qos,
    boolean retain,
    String topicName,
    int packetId,
    byte[] payload,
    MqttProperties properties
) implements MqttPacket {

    public PublishPacket {
        Objects.requireNonNull(qos, "qos must not be null");
        Objects.requireNonNull(topicName, "topicName must not be null");
        if (payload == null) {
            payload = new byte[0];
        }
        if (qos == MqttQoS.AT_MOST_ONCE && packetId != 0) {
            throw new IllegalArgumentException("Packet ID must be 0 for QoS 0, got: " + packetId);
        }
        if (qos != MqttQoS.AT_MOST_ONCE && packetId == 0) {
            throw new IllegalArgumentException("Packet ID must be non-zero for QoS " + qos.value());
        }
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.PUBLISH;
    }
}
