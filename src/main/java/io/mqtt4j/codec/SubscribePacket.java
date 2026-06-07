package io.mqtt4j.codec;

import io.mqtt4j.message.MqttQoS;

import java.util.List;
import java.util.Objects;

/**
 * MQTT SUBSCRIBE Control Packet.
 *
 * <p>Sent by the client to subscribe to one or more topic filters. The fixed
 * header flags are reserved as {@code 0010}. In v5.0, subscription options
 * include No Local, Retain As Published, and Retain Handling in addition to
 * the requested QoS.</p>
 *
 * @param packetId      the packet identifier
 * @param subscriptions the list of topic subscriptions (non-empty)
 * @param properties    the v5.0 properties ({@code null} for v3.1.1)
 */
public record SubscribePacket(
    int packetId,
    List<Subscription> subscriptions,
    MqttProperties properties
) implements MqttPacket {

    public SubscribePacket {
        Objects.requireNonNull(subscriptions, "subscriptions must not be null");
        if (subscriptions.isEmpty()) {
            throw new IllegalArgumentException("subscriptions must not be empty");
        }
        subscriptions = List.copyOf(subscriptions);
    }

    @Override
    public MqttPacketType type() {
        return MqttPacketType.SUBSCRIBE;
    }

    /**
     * Represents a single topic subscription with QoS and v5.0 options.
     *
     * @param topicFilter       the topic filter string
     * @param requestedQos      the maximum QoS level requested
     * @param noLocal           v5.0: if true, the server must not forward messages published
     *                          by this client back to this client
     * @param retainAsPublished v5.0: if true, the server keeps the RETAIN flag as published
     * @param retainHandling    v5.0: controls when retained messages are sent (0, 1, or 2)
     */
    public record Subscription(
        String topicFilter,
        MqttQoS requestedQos,
        boolean noLocal,
        boolean retainAsPublished,
        int retainHandling
    ) {
        public Subscription {
            Objects.requireNonNull(topicFilter, "topicFilter must not be null");
            Objects.requireNonNull(requestedQos, "requestedQos must not be null");
            if (retainHandling < 0 || retainHandling > 2) {
                throw new IllegalArgumentException("retainHandling must be 0, 1, or 2, got: " + retainHandling);
            }
        }

        /**
         * Creates a v3.1.1-compatible subscription (no v5.0 subscription options).
         *
         * @param topicFilter  the topic filter
         * @param requestedQos the requested maximum QoS
         */
        public static Subscription of(String topicFilter, MqttQoS requestedQos) {
            return new Subscription(topicFilter, requestedQos, false, false, 0);
        }

        /**
         * Encodes the subscription options byte.
         *
         * <pre>
         * Bit 7-6: Reserved (0)
         * Bit 5-4: Retain Handling
         * Bit 3:   Retain As Published
         * Bit 2:   No Local
         * Bit 1-0: QoS
         * </pre>
         */
        public int optionsByte() {
            int b = requestedQos.value() & 0x03;
            if (noLocal) b |= 0x04;
            if (retainAsPublished) b |= 0x08;
            b |= (retainHandling & 0x03) << 4;
            return b;
        }
    }
}
