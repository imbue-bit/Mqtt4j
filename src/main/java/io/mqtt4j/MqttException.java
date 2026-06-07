package io.mqtt4j;

/**
 * Base exception for all MQTT client errors.
 */
public class MqttException extends RuntimeException {

    private final int reasonCode;

    public MqttException(String message) {
        super(message);
        this.reasonCode = -1;
    }

    public MqttException(String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = -1;
    }

    public MqttException(int reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public MqttException(int reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
    }

    /**
     * Returns the MQTT reason code, or {@code -1} if not applicable.
     */
    public int reasonCode() {
        return reasonCode;
    }

    // ── Specific exception subtypes ──────────────────────────────────────

    /** Thrown when the connection to the broker fails or is rejected. */
    public static class ConnectionException extends MqttException {
        public ConnectionException(String message) { super(message); }
        public ConnectionException(String message, Throwable cause) { super(message, cause); }
        public ConnectionException(int reasonCode, String message) { super(reasonCode, message); }
    }

    /** Thrown when the connection is unexpectedly lost. */
    public static class ConnectionLostException extends MqttException {
        public ConnectionLostException(String message) { super(message); }
        public ConnectionLostException(String message, Throwable cause) { super(message, cause); }
    }

    /** Thrown when a protocol-level error is detected. */
    public static class ProtocolException extends MqttException {
        public ProtocolException(String message) { super(message); }
        public ProtocolException(int reasonCode, String message) { super(reasonCode, message); }
    }

    /** Thrown when a packet encoding or decoding error occurs. */
    public static class CodecException extends MqttException {
        public CodecException(String message) { super(message); }
        public CodecException(String message, Throwable cause) { super(message, cause); }
    }

    /** Thrown when an operation times out. */
    public static class TimeoutException extends MqttException {
        public TimeoutException(String message) { super(message); }
    }
}
