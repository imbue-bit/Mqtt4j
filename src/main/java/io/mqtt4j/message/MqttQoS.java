package io.mqtt4j.message;

/**
 * MQTT Quality of Service levels.
 */
public enum MqttQoS {

    /** At most once delivery (fire and forget). */
    AT_MOST_ONCE(0),

    /** At least once delivery (acknowledged delivery). */
    AT_LEAST_ONCE(1),

    /** Exactly once delivery (assured delivery via 4-step handshake). */
    EXACTLY_ONCE(2);

    private final int value;

    MqttQoS(int value) {
        this.value = value;
    }

    /** Returns the numeric QoS value (0, 1, or 2). */
    public int value() {
        return value;
    }

    /** Resolves a numeric QoS value to the corresponding enum constant. */
    public static MqttQoS valueOf(int value) {
        return switch (value) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            case 2 -> EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("Invalid QoS value: " + value);
        };
    }
}
