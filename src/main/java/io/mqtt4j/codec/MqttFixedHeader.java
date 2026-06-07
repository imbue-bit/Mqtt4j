package io.mqtt4j.codec;

import io.mqtt4j.message.MqttQoS;

/**
 * Represents the MQTT fixed header present in every control packet.
 *
 * <pre>
 * Bit    7  6  5  4    3  2  1  0
 *       ├──────────┤  ├──────────┤
 *       Packet Type    Flags
 *        (4 bits)     DUP QoS Retain
 * </pre>
 *
 * @param packetType      the packet type
 * @param dup             duplicate delivery flag (only meaningful for PUBLISH)
 * @param qos             quality of service level (only meaningful for PUBLISH)
 * @param retain          retain flag (only meaningful for PUBLISH)
 * @param remainingLength the remaining length of the packet (variable header + payload)
 */
public record MqttFixedHeader(
    MqttPacketType packetType,
    boolean dup,
    MqttQoS qos,
    boolean retain,
    int remainingLength
) {

    /**
     * Creates a fixed header for non-PUBLISH packets (flags derived from packet type).
     */
    public static MqttFixedHeader forPacketType(MqttPacketType type, int remainingLength) {
        return new MqttFixedHeader(type, false, MqttQoS.AT_MOST_ONCE, false, remainingLength);
    }

    /**
     * Creates a fixed header for a PUBLISH packet.
     */
    public static MqttFixedHeader forPublish(boolean dup, MqttQoS qos, boolean retain, int remainingLength) {
        return new MqttFixedHeader(MqttPacketType.PUBLISH, dup, qos, retain, remainingLength);
    }

    /**
     * Encodes the first byte of the fixed header.
     */
    public int encodedFirstByte() {
        if (packetType == MqttPacketType.PUBLISH) {
            int flags = 0;
            if (dup) flags |= 0x08;
            flags |= (qos.value() << 1);
            if (retain) flags |= 0x01;
            return (packetType.value() << 4) | flags;
        }
        return packetType.fixedHeaderByte();
    }

    /**
     * Decodes a fixed header from the first byte value.
     *
     * @param firstByte       the first byte of the packet
     * @param remainingLength the decoded remaining length
     */
    public static MqttFixedHeader decode(int firstByte, int remainingLength) {
        int typeValue = (firstByte >> 4) & 0x0F;
        MqttPacketType type = MqttPacketType.fromValue(typeValue);

        if (type == MqttPacketType.PUBLISH) {
            boolean dup = (firstByte & 0x08) != 0;
            MqttQoS qos = MqttQoS.valueOf((firstByte >> 1) & 0x03);
            boolean retain = (firstByte & 0x01) != 0;
            return new MqttFixedHeader(type, dup, qos, retain, remainingLength);
        }

        return new MqttFixedHeader(type, false, MqttQoS.AT_MOST_ONCE, false, remainingLength);
    }
}
