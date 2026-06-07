package io.mqtt4j.codec;

import io.mqtt4j.MqttException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * MQTT v5.0 Properties container.
 *
 * <p>Provides typed accessors for all standard MQTT v5.0 properties and
 * wire-format encoding/decoding.</p>
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * MqttProperties props = new MqttProperties()
 *     .setSessionExpiryInterval(3600)
 *     .setReceiveMaximum(100)
 *     .addUserProperty("app", "mqtt4j");
 * }</pre>
 */
public final class MqttProperties {

    // ── Property ID Constants ──────────────────────────────────────────────

    public static final int PAYLOAD_FORMAT_INDICATOR      = 0x01;
    public static final int MESSAGE_EXPIRY_INTERVAL       = 0x02;
    public static final int CONTENT_TYPE                  = 0x03;
    public static final int RESPONSE_TOPIC                = 0x08;
    public static final int CORRELATION_DATA              = 0x09;
    public static final int SUBSCRIPTION_IDENTIFIER       = 0x0B;
    public static final int SESSION_EXPIRY_INTERVAL       = 0x11;
    public static final int ASSIGNED_CLIENT_IDENTIFIER    = 0x12;
    public static final int SERVER_KEEP_ALIVE             = 0x13;
    public static final int AUTHENTICATION_METHOD         = 0x15;
    public static final int AUTHENTICATION_DATA           = 0x16;
    public static final int REQUEST_PROBLEM_INFORMATION   = 0x17;
    public static final int WILL_DELAY_INTERVAL           = 0x18;
    public static final int REQUEST_RESPONSE_INFORMATION  = 0x19;
    public static final int RESPONSE_INFORMATION          = 0x1A;
    public static final int SERVER_REFERENCE              = 0x1C;
    public static final int REASON_STRING                 = 0x1F;
    public static final int RECEIVE_MAXIMUM               = 0x21;
    public static final int TOPIC_ALIAS_MAXIMUM           = 0x22;
    public static final int TOPIC_ALIAS                   = 0x23;
    public static final int MAXIMUM_QOS                   = 0x24;
    public static final int RETAIN_AVAILABLE              = 0x25;
    public static final int USER_PROPERTY                 = 0x26;
    public static final int MAXIMUM_PACKET_SIZE           = 0x27;
    public static final int WILDCARD_SUBSCRIPTION_AVAILABLE      = 0x28;
    public static final int SUBSCRIPTION_IDENTIFIERS_AVAILABLE   = 0x29;
    public static final int SHARED_SUBSCRIPTION_AVAILABLE        = 0x2A;

    /** Represents a key-value string pair used for User Properties. */
    public record StringPair(String key, String value) {}

    // ── Property data type classification ──────────────────────────────────

    private enum PropertyDataType {
        BYTE, TWO_BYTE_INTEGER, FOUR_BYTE_INTEGER,
        VARIABLE_BYTE_INTEGER, UTF8_STRING, BINARY_DATA, UTF8_STRING_PAIR
    }

    private static PropertyDataType dataTypeOf(int propertyId) {
        return switch (propertyId) {
            case PAYLOAD_FORMAT_INDICATOR, REQUEST_PROBLEM_INFORMATION,
                 REQUEST_RESPONSE_INFORMATION, MAXIMUM_QOS, RETAIN_AVAILABLE,
                 WILDCARD_SUBSCRIPTION_AVAILABLE, SUBSCRIPTION_IDENTIFIERS_AVAILABLE,
                 SHARED_SUBSCRIPTION_AVAILABLE -> PropertyDataType.BYTE;
            case SERVER_KEEP_ALIVE, RECEIVE_MAXIMUM, TOPIC_ALIAS_MAXIMUM,
                 TOPIC_ALIAS -> PropertyDataType.TWO_BYTE_INTEGER;
            case MESSAGE_EXPIRY_INTERVAL, SESSION_EXPIRY_INTERVAL,
                 WILL_DELAY_INTERVAL, MAXIMUM_PACKET_SIZE -> PropertyDataType.FOUR_BYTE_INTEGER;
            case SUBSCRIPTION_IDENTIFIER -> PropertyDataType.VARIABLE_BYTE_INTEGER;
            case CONTENT_TYPE, RESPONSE_TOPIC, ASSIGNED_CLIENT_IDENTIFIER,
                 AUTHENTICATION_METHOD, RESPONSE_INFORMATION, SERVER_REFERENCE,
                 REASON_STRING -> PropertyDataType.UTF8_STRING;
            case CORRELATION_DATA, AUTHENTICATION_DATA -> PropertyDataType.BINARY_DATA;
            case USER_PROPERTY -> PropertyDataType.UTF8_STRING_PAIR;
            default -> throw new MqttException.CodecException(
                "Unknown MQTT property ID: 0x" + Integer.toHexString(propertyId));
        };
    }

    // ── Internal Storage ───────────────────────────────────────────────────

    private final Map<Integer, Object> values = new LinkedHashMap<>();
    private final List<StringPair> userProperties = new ArrayList<>();

    /** An immutable empty properties instance. */
    public static final MqttProperties EMPTY = new MqttProperties();

    public MqttProperties() {}

    public boolean isEmpty() {
        return values.isEmpty() && userProperties.isEmpty();
    }

    // ── Generic accessors ──────────────────────────────────────────────────

    public MqttProperties set(int propertyId, Object value) {
        if (propertyId == USER_PROPERTY) {
            throw new IllegalArgumentException("Use addUserProperty() for User Properties");
        }
        values.put(propertyId, value);
        return this;
    }

    public Object getRaw(int propertyId) {
        return values.get(propertyId);
    }

    // ── Typed setters ──────────────────────────────────────────────────────

    public MqttProperties setPayloadFormatIndicator(int value) { return set(PAYLOAD_FORMAT_INDICATOR, value); }
    public MqttProperties setMessageExpiryInterval(long value) { return set(MESSAGE_EXPIRY_INTERVAL, value); }
    public MqttProperties setContentType(String value) { return set(CONTENT_TYPE, value); }
    public MqttProperties setResponseTopic(String value) { return set(RESPONSE_TOPIC, value); }
    public MqttProperties setCorrelationData(byte[] value) { return set(CORRELATION_DATA, value); }
    public MqttProperties setSubscriptionIdentifier(int value) { return set(SUBSCRIPTION_IDENTIFIER, value); }
    public MqttProperties setSessionExpiryInterval(long value) { return set(SESSION_EXPIRY_INTERVAL, value); }
    public MqttProperties setAssignedClientIdentifier(String value) { return set(ASSIGNED_CLIENT_IDENTIFIER, value); }
    public MqttProperties setServerKeepAlive(int value) { return set(SERVER_KEEP_ALIVE, value); }
    public MqttProperties setAuthenticationMethod(String value) { return set(AUTHENTICATION_METHOD, value); }
    public MqttProperties setAuthenticationData(byte[] value) { return set(AUTHENTICATION_DATA, value); }
    public MqttProperties setRequestProblemInformation(int value) { return set(REQUEST_PROBLEM_INFORMATION, value); }
    public MqttProperties setWillDelayInterval(long value) { return set(WILL_DELAY_INTERVAL, value); }
    public MqttProperties setRequestResponseInformation(int value) { return set(REQUEST_RESPONSE_INFORMATION, value); }
    public MqttProperties setResponseInformation(String value) { return set(RESPONSE_INFORMATION, value); }
    public MqttProperties setServerReference(String value) { return set(SERVER_REFERENCE, value); }
    public MqttProperties setReasonString(String value) { return set(REASON_STRING, value); }
    public MqttProperties setReceiveMaximum(int value) { return set(RECEIVE_MAXIMUM, value); }
    public MqttProperties setTopicAliasMaximum(int value) { return set(TOPIC_ALIAS_MAXIMUM, value); }
    public MqttProperties setTopicAlias(int value) { return set(TOPIC_ALIAS, value); }
    public MqttProperties setMaximumQoS(int value) { return set(MAXIMUM_QOS, value); }
    public MqttProperties setRetainAvailable(boolean value) { return set(RETAIN_AVAILABLE, value ? 1 : 0); }
    public MqttProperties setMaximumPacketSize(long value) { return set(MAXIMUM_PACKET_SIZE, value); }
    public MqttProperties setWildcardSubscriptionAvailable(boolean value) { return set(WILDCARD_SUBSCRIPTION_AVAILABLE, value ? 1 : 0); }
    public MqttProperties setSubscriptionIdentifiersAvailable(boolean value) { return set(SUBSCRIPTION_IDENTIFIERS_AVAILABLE, value ? 1 : 0); }
    public MqttProperties setSharedSubscriptionAvailable(boolean value) { return set(SHARED_SUBSCRIPTION_AVAILABLE, value ? 1 : 0); }

    public MqttProperties addUserProperty(String key, String value) {
        userProperties.add(new StringPair(key, value));
        return this;
    }

    // ── Typed getters ──────────────────────────────────────────────────────

    public OptionalInt getPayloadFormatIndicator() { return getAsInt(PAYLOAD_FORMAT_INDICATOR); }
    public OptionalLong getMessageExpiryInterval() { return getAsLong(MESSAGE_EXPIRY_INTERVAL); }
    public Optional<String> getContentType() { return getAsString(CONTENT_TYPE); }
    public Optional<String> getResponseTopic() { return getAsString(RESPONSE_TOPIC); }
    public Optional<byte[]> getCorrelationData() { return getAsBytes(CORRELATION_DATA); }
    public OptionalInt getSubscriptionIdentifier() { return getAsInt(SUBSCRIPTION_IDENTIFIER); }
    public OptionalLong getSessionExpiryInterval() { return getAsLong(SESSION_EXPIRY_INTERVAL); }
    public Optional<String> getAssignedClientIdentifier() { return getAsString(ASSIGNED_CLIENT_IDENTIFIER); }
    public OptionalInt getServerKeepAlive() { return getAsInt(SERVER_KEEP_ALIVE); }
    public Optional<String> getAuthenticationMethod() { return getAsString(AUTHENTICATION_METHOD); }
    public Optional<byte[]> getAuthenticationData() { return getAsBytes(AUTHENTICATION_DATA); }
    public OptionalInt getRequestProblemInformation() { return getAsInt(REQUEST_PROBLEM_INFORMATION); }
    public OptionalLong getWillDelayInterval() { return getAsLong(WILL_DELAY_INTERVAL); }
    public OptionalInt getRequestResponseInformation() { return getAsInt(REQUEST_RESPONSE_INFORMATION); }
    public Optional<String> getResponseInformation() { return getAsString(RESPONSE_INFORMATION); }
    public Optional<String> getServerReference() { return getAsString(SERVER_REFERENCE); }
    public Optional<String> getReasonString() { return getAsString(REASON_STRING); }
    public OptionalInt getReceiveMaximum() { return getAsInt(RECEIVE_MAXIMUM); }
    public OptionalInt getTopicAliasMaximum() { return getAsInt(TOPIC_ALIAS_MAXIMUM); }
    public OptionalInt getTopicAlias() { return getAsInt(TOPIC_ALIAS); }
    public OptionalInt getMaximumQoS() { return getAsInt(MAXIMUM_QOS); }
    public OptionalInt getRetainAvailable() { return getAsInt(RETAIN_AVAILABLE); }
    public OptionalLong getMaximumPacketSize() { return getAsLong(MAXIMUM_PACKET_SIZE); }
    public OptionalInt getWildcardSubscriptionAvailable() { return getAsInt(WILDCARD_SUBSCRIPTION_AVAILABLE); }
    public OptionalInt getSubscriptionIdentifiersAvailable() { return getAsInt(SUBSCRIPTION_IDENTIFIERS_AVAILABLE); }
    public OptionalInt getSharedSubscriptionAvailable() { return getAsInt(SHARED_SUBSCRIPTION_AVAILABLE); }

    public List<StringPair> getUserProperties() {
        return Collections.unmodifiableList(userProperties);
    }

    // ── Internal helpers ───────────────────────────────────────────────────

    private OptionalInt getAsInt(int id) {
        Object v = values.get(id);
        if (v == null) return OptionalInt.empty();
        return OptionalInt.of(((Number) v).intValue());
    }

    private OptionalLong getAsLong(int id) {
        Object v = values.get(id);
        if (v == null) return OptionalLong.empty();
        return OptionalLong.of(((Number) v).longValue());
    }

    private Optional<String> getAsString(int id) {
        return Optional.ofNullable((String) values.get(id));
    }

    private Optional<byte[]> getAsBytes(int id) {
        return Optional.ofNullable((byte[]) values.get(id));
    }

    // ── Wire Format Encoding ───────────────────────────────────────────────

    /**
     * Encodes this properties section to the output stream.
     * Format: Property Length (VBI) + [Property ID (VBI) + Value]*
     */
    public void encode(OutputStream out) throws IOException {
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        for (var entry : values.entrySet()) {
            encodeProperty(content, entry.getKey(), entry.getValue());
        }
        for (StringPair pair : userProperties) {
            MqttCodecUtil.encodeVariableByteInteger(content, USER_PROPERTY);
            MqttCodecUtil.encodeUtf8String(content, pair.key());
            MqttCodecUtil.encodeUtf8String(content, pair.value());
        }
        byte[] encoded = content.toByteArray();
        MqttCodecUtil.encodeVariableByteInteger(out, encoded.length);
        out.write(encoded);
    }

    /**
     * Returns the encoded properties as a byte array (including the length prefix).
     */
    public byte[] toBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            encode(baos);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new MqttException.CodecException("Failed to encode properties", e);
        }
    }

    private void encodeProperty(OutputStream out, int id, Object value) throws IOException {
        MqttCodecUtil.encodeVariableByteInteger(out, id);
        PropertyDataType type = dataTypeOf(id);
        switch (type) {
            case BYTE -> out.write(((Number) value).intValue() & 0xFF);
            case TWO_BYTE_INTEGER -> MqttCodecUtil.encodeUnsignedShort(out, ((Number) value).intValue());
            case FOUR_BYTE_INTEGER -> MqttCodecUtil.encodeUnsignedInt(out, ((Number) value).longValue());
            case VARIABLE_BYTE_INTEGER -> MqttCodecUtil.encodeVariableByteInteger(out, ((Number) value).intValue());
            case UTF8_STRING -> MqttCodecUtil.encodeUtf8String(out, (String) value);
            case BINARY_DATA -> MqttCodecUtil.encodeBinaryData(out, (byte[]) value);
            case UTF8_STRING_PAIR -> throw new IllegalStateException("User properties handled separately");
        }
    }

    // ── Wire Format Decoding ───────────────────────────────────────────────

    /**
     * Decodes properties from the input stream.
     */
    public static MqttProperties decode(InputStream in) throws IOException {
        int propertyLength = MqttCodecUtil.decodeVariableByteInteger(in);
        if (propertyLength == 0) {
            return new MqttProperties();
        }

        byte[] propertyBytes = MqttCodecUtil.readBytes(in, propertyLength);
        ByteArrayInputStream propStream = new ByteArrayInputStream(propertyBytes);
        MqttProperties props = new MqttProperties();

        while (propStream.available() > 0) {
            int propertyId = MqttCodecUtil.decodeVariableByteInteger(propStream);
            PropertyDataType type = dataTypeOf(propertyId);
            switch (type) {
                case BYTE -> props.values.put(propertyId, MqttCodecUtil.readByte(propStream));
                case TWO_BYTE_INTEGER -> props.values.put(propertyId, MqttCodecUtil.decodeUnsignedShort(propStream));
                case FOUR_BYTE_INTEGER -> props.values.put(propertyId, MqttCodecUtil.decodeUnsignedInt(propStream));
                case VARIABLE_BYTE_INTEGER -> props.values.put(propertyId, MqttCodecUtil.decodeVariableByteInteger(propStream));
                case UTF8_STRING -> props.values.put(propertyId, MqttCodecUtil.decodeUtf8String(propStream));
                case BINARY_DATA -> props.values.put(propertyId, MqttCodecUtil.decodeBinaryData(propStream));
                case UTF8_STRING_PAIR -> {
                    String key = MqttCodecUtil.decodeUtf8String(propStream);
                    String val = MqttCodecUtil.decodeUtf8String(propStream);
                    props.userProperties.add(new StringPair(key, val));
                }
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "MqttProperties{values=" + values.size() +
               ", userProperties=" + userProperties.size() + '}';
    }
}
