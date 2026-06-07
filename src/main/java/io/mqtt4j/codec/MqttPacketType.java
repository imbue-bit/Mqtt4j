package io.mqtt4j.codec;

/**
 * MQTT Control Packet types as defined in the MQTT specification.
 *
 * <p>Each type has a unique 4-bit value (1-15) used in the fixed header,
 * and specific flags that must be set in the fixed header byte.</p>
 */
public enum MqttPacketType {

    CONNECT     (1,  0b0000),
    CONNACK     (2,  0b0000),
    PUBLISH     (3,  0b0000),  // flags are variable for PUBLISH
    PUBACK      (4,  0b0000),
    PUBREC      (5,  0b0000),
    PUBREL      (6,  0b0010),  // reserved flags = 0010
    PUBCOMP     (7,  0b0000),
    SUBSCRIBE   (8,  0b0010),  // reserved flags = 0010
    SUBACK      (9,  0b0000),
    UNSUBSCRIBE (10, 0b0010),  // reserved flags = 0010
    UNSUBACK    (11, 0b0000),
    PINGREQ     (12, 0b0000),
    PINGRESP    (13, 0b0000),
    DISCONNECT  (14, 0b0000),
    AUTH        (15, 0b0000);  // MQTT v5.0 only

    private final int value;
    private final int fixedFlags;

    MqttPacketType(int value, int fixedFlags) {
        this.value = value;
        this.fixedFlags = fixedFlags;
    }

    /** Returns the 4-bit packet type value. */
    public int value() {
        return value;
    }

    /** Returns the fixed flags for this packet type (lower 4 bits of byte 1). */
    public int fixedFlags() {
        return fixedFlags;
    }

    /** Returns the first byte of the fixed header (type << 4 | flags). */
    public int fixedHeaderByte() {
        return (value << 4) | fixedFlags;
    }

    private static final MqttPacketType[] VALUES_BY_TYPE = new MqttPacketType[16];
    static {
        for (MqttPacketType type : values()) {
            VALUES_BY_TYPE[type.value] = type;
        }
    }

    /**
     * Resolves a 4-bit packet type value to the corresponding enum constant.
     *
     * @param typeValue the packet type value (1–15)
     * @return the corresponding packet type
     * @throws IllegalArgumentException if the value is out of range or reserved
     */
    public static MqttPacketType fromValue(int typeValue) {
        if (typeValue < 1 || typeValue > 15 || VALUES_BY_TYPE[typeValue] == null) {
            throw new IllegalArgumentException("Unknown MQTT packet type: " + typeValue);
        }
        return VALUES_BY_TYPE[typeValue];
    }
}
