package io.mqtt4j;

/**
 * MQTT protocol version.
 */
public enum MqttVersion {

    /** MQTT v3.1.1 (protocol level 4). */
    V3_1_1(4, "MQTT", "3.1.1"),

    /** MQTT v5.0 (protocol level 5). */
    V5_0(5, "MQTT", "5.0");

    private final int protocolLevel;
    private final String protocolName;
    private final String displayName;

    MqttVersion(int protocolLevel, String protocolName, String displayName) {
        this.protocolLevel = protocolLevel;
        this.protocolName = protocolName;
        this.displayName = displayName;
    }

    /** Returns the protocol level byte value (4 for v3.1.1, 5 for v5.0). */
    public int protocolLevel() {
        return protocolLevel;
    }

    /** Returns the protocol name used in the CONNECT packet ("MQTT"). */
    public String protocolName() {
        return protocolName;
    }

    /** Returns a human-readable display name. */
    public String displayName() {
        return displayName;
    }

    /** Resolves a protocol level byte to the corresponding enum value. */
    public static MqttVersion fromProtocolLevel(int level) {
        return switch (level) {
            case 4 -> V3_1_1;
            case 5 -> V5_0;
            default -> throw new IllegalArgumentException("Unsupported MQTT protocol level: " + level);
        };
    }

    @Override
    public String toString() {
        return "MQTT " + displayName;
    }
}
