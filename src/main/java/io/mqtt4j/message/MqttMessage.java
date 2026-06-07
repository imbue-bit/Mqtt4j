package io.mqtt4j.message;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Represents an MQTT message with topic, payload, QoS, and retain flag.
 *
 * <p>This is the user-facing message type delivered via {@link MqttMessageHandler}
 * and used for publishing.</p>
 */
public final class MqttMessage {

    private final String topic;
    private final byte[] payload;
    private final MqttQoS qos;
    private final boolean retain;

    public MqttMessage(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        this.topic = topic;
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.qos = qos;
        this.retain = retain;
    }

    /** Creates a QoS 0, non-retained message. */
    public static MqttMessage of(String topic, byte[] payload) {
        return new MqttMessage(topic, payload, MqttQoS.AT_MOST_ONCE, false);
    }

    /** Creates a QoS 0, non-retained message from a string payload. */
    public static MqttMessage of(String topic, String payload) {
        return of(topic, payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns the topic name. */
    public String topic() {
        return topic;
    }

    /** Returns a copy of the payload bytes. */
    public byte[] payload() {
        return payload.clone();
    }

    /** Returns the payload as a UTF-8 string. */
    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /** Returns the payload length in bytes. */
    public int payloadLength() {
        return payload.length;
    }

    /** Returns the QoS level. */
    public MqttQoS qos() {
        return qos;
    }

    /** Returns {@code true} if this is a retained message. */
    public boolean isRetain() {
        return retain;
    }

    @Override
    public String toString() {
        return "MqttMessage{topic='" + topic + "', qos=" + qos +
               ", retain=" + retain + ", payloadLength=" + payload.length + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MqttMessage that)) return false;
        return retain == that.retain &&
               topic.equals(that.topic) &&
               Arrays.equals(payload, that.payload) &&
               qos == that.qos;
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + Arrays.hashCode(payload);
        result = 31 * result + qos.hashCode();
        result = 31 * result + Boolean.hashCode(retain);
        return result;
    }
}
