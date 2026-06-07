package io.mqtt4j.codec;

/**
 * MQTT v5.0 Reason Codes.
 *
 * <p>Reason codes below {@code 0x80} indicate success; codes {@code 0x80}
 * and above indicate failure.</p>
 */
public enum MqttReasonCode {

    // ── Success codes ──────────────────────────────────────────────────────
    SUCCESS                             (0x00, "Success"),
    NORMAL_DISCONNECTION                (0x00, "Normal disconnection"),
    GRANTED_QOS_0                       (0x00, "Granted QoS 0"),
    GRANTED_QOS_1                       (0x01, "Granted QoS 1"),
    GRANTED_QOS_2                       (0x02, "Granted QoS 2"),
    DISCONNECT_WITH_WILL_MESSAGE        (0x04, "Disconnect with Will Message"),
    NO_MATCHING_SUBSCRIBERS             (0x10, "No matching subscribers"),
    NO_SUBSCRIPTION_EXISTED             (0x11, "No subscription existed"),
    CONTINUE_AUTHENTICATION             (0x18, "Continue authentication"),
    RE_AUTHENTICATE                     (0x19, "Re-authenticate"),

    // ── Error codes (>= 0x80) ──────────────────────────────────────────────
    UNSPECIFIED_ERROR                   (0x80, "Unspecified error"),
    MALFORMED_PACKET                    (0x81, "Malformed Packet"),
    PROTOCOL_ERROR                      (0x82, "Protocol Error"),
    IMPLEMENTATION_SPECIFIC_ERROR       (0x83, "Implementation specific error"),
    UNSUPPORTED_PROTOCOL_VERSION        (0x84, "Unsupported Protocol Version"),
    CLIENT_IDENTIFIER_NOT_VALID         (0x85, "Client Identifier not valid"),
    BAD_USER_NAME_OR_PASSWORD           (0x86, "Bad User Name or Password"),
    NOT_AUTHORIZED                      (0x87, "Not authorized"),
    SERVER_UNAVAILABLE                  (0x88, "Server unavailable"),
    SERVER_BUSY                         (0x89, "Server busy"),
    BANNED                              (0x8A, "Banned"),
    SERVER_SHUTTING_DOWN                (0x8B, "Server shutting down"),
    BAD_AUTHENTICATION_METHOD           (0x8C, "Bad authentication method"),
    KEEP_ALIVE_TIMEOUT                  (0x8D, "Keep Alive timeout"),
    SESSION_TAKEN_OVER                  (0x8E, "Session taken over"),
    TOPIC_FILTER_INVALID                (0x8F, "Topic Filter invalid"),
    TOPIC_NAME_INVALID                  (0x90, "Topic Name invalid"),
    PACKET_IDENTIFIER_IN_USE            (0x91, "Packet Identifier in use"),
    PACKET_IDENTIFIER_NOT_FOUND         (0x92, "Packet Identifier not found"),
    RECEIVE_MAXIMUM_EXCEEDED            (0x93, "Receive Maximum exceeded"),
    TOPIC_ALIAS_INVALID                 (0x94, "Topic Alias invalid"),
    PACKET_TOO_LARGE                    (0x95, "Packet too large"),
    MESSAGE_RATE_TOO_HIGH               (0x96, "Message rate too high"),
    QUOTA_EXCEEDED                      (0x97, "Quota exceeded"),
    ADMINISTRATIVE_ACTION               (0x98, "Administrative action"),
    PAYLOAD_FORMAT_INVALID              (0x99, "Payload format invalid"),
    RETAIN_NOT_SUPPORTED                (0x9A, "Retain not supported"),
    QOS_NOT_SUPPORTED                   (0x9B, "QoS not supported"),
    USE_ANOTHER_SERVER                  (0x9C, "Use another server"),
    SERVER_MOVED                        (0x9D, "Server moved"),
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED  (0x9E, "Shared Subscriptions not supported"),
    CONNECTION_RATE_EXCEEDED            (0x9F, "Connection rate exceeded"),
    MAXIMUM_CONNECT_TIME                (0xA0, "Maximum connect time"),
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED(0xA1, "Subscription Identifiers not supported"),
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED(0xA2, "Wildcard Subscriptions not supported");

    private final int code;
    private final String description;

    MqttReasonCode(int code, String description) {
        this.code = code;
        this.description = description;
    }

    /** Returns the numeric reason code byte value. */
    public int code() {
        return code;
    }

    /** Returns a human-readable description. */
    public String description() {
        return description;
    }

    /** Returns {@code true} if this is an error code (>= 0x80). */
    public boolean isError() {
        return code >= 0x80;
    }

    /**
     * Resolves a reason code byte value. Returns {@code null} if not recognized.
     */
    public static MqttReasonCode fromCode(int code) {
        for (MqttReasonCode rc : values()) {
            if (rc.code == code) return rc;
        }
        return null;
    }

    /**
     * Resolves a reason code byte value, throwing on unknown codes.
     */
    public static MqttReasonCode fromCodeOrThrow(int code) {
        MqttReasonCode rc = fromCode(code);
        if (rc == null) {
            throw new IllegalArgumentException("Unknown MQTT reason code: 0x" +
                Integer.toHexString(code));
        }
        return rc;
    }

    @Override
    public String toString() {
        return String.format("0x%02X (%s)", code, description);
    }
}
