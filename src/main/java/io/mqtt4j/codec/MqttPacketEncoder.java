package io.mqtt4j.codec;

import io.mqtt4j.MqttException;
import io.mqtt4j.MqttVersion;
import io.mqtt4j.message.MqttQoS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Encodes {@link MqttPacket} instances into their MQTT wire format.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * byte[] bytes = MqttPacketEncoder.encode(connectPacket, MqttVersion.V3_1_1);
 * }</pre>
 *
 * <p>The encoder handles both MQTT v3.1.1 and v5.0 formats. For v5.0 packets,
 * properties are included after the variable header. For v3.1.1, properties
 * are omitted.</p>
 */
public final class MqttPacketEncoder {

    private MqttPacketEncoder() {} // utility class

    /**
     * Encodes an MQTT packet into its complete wire-format byte array.
     *
     * @param packet  the packet to encode
     * @param version the target MQTT protocol version
     * @return the encoded bytes (fixed header + variable header + payload)
     * @throws MqttException.CodecException if encoding fails
     */
    public static byte[] encode(MqttPacket packet, MqttVersion version) {
        try {
            return switch (packet) {
                case ConnectPacket p    -> encodeConnect(p);
                case ConnAckPacket p    -> encodeConnAck(p, version);
                case PublishPacket p    -> encodePublish(p, version);
                case PubAckPacket p     -> encodePubAckStyle(MqttPacketType.PUBACK, p.packetId(), p.reasonCode(), p.properties(), version);
                case PubRecPacket p     -> encodePubAckStyle(MqttPacketType.PUBREC, p.packetId(), p.reasonCode(), p.properties(), version);
                case PubRelPacket p     -> encodePubAckStyle(MqttPacketType.PUBREL, p.packetId(), p.reasonCode(), p.properties(), version);
                case PubCompPacket p    -> encodePubAckStyle(MqttPacketType.PUBCOMP, p.packetId(), p.reasonCode(), p.properties(), version);
                case SubscribePacket p  -> encodeSubscribe(p, version);
                case SubAckPacket p     -> encodeSubAck(p, version);
                case UnsubscribePacket p -> encodeUnsubscribe(p, version);
                case UnsubAckPacket p   -> encodeUnsubAck(p, version);
                case PingReqPacket p    -> new byte[]{(byte) 0xC0, 0x00};
                case PingRespPacket p   -> new byte[]{(byte) 0xD0, 0x00};
                case DisconnectPacket p -> encodeDisconnect(p, version);
                case AuthPacket p       -> encodeAuth(p);
            };
        } catch (IOException e) {
            throw new MqttException.CodecException("Failed to encode " + packet.type(), e);
        }
    }

    // ── CONNECT ───────────────────────────────────────────────────────────

    private static byte[] encodeConnect(ConnectPacket p) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Protocol Name: "MQTT" as UTF-8 string
        MqttCodecUtil.encodeUtf8String(varHeader, p.version().protocolName());
        // Protocol Level
        varHeader.write(p.version().protocolLevel());
        // Connect Flags
        varHeader.write(p.connectFlagsByte());
        // Keep Alive
        MqttCodecUtil.encodeUnsignedShort(varHeader, p.keepAliveSeconds());

        // v5.0 properties
        if (p.version() == MqttVersion.V5_0) {
            encodeProperties(varHeader, p.properties());
        }

        // ── Payload ────────────────────────────────────────────────────────
        ByteArrayOutputStream payload = new ByteArrayOutputStream();

        // Client Identifier
        MqttCodecUtil.encodeUtf8String(payload, p.clientId());

        // Will
        if (p.hasWill()) {
            // v5.0 will properties
            if (p.version() == MqttVersion.V5_0) {
                encodeProperties(payload, p.willProperties());
            }
            MqttCodecUtil.encodeUtf8String(payload, p.willTopic());
            MqttCodecUtil.encodeBinaryData(payload, p.willMessage() != null ? p.willMessage() : new byte[0]);
        }

        // Username
        if (p.hasUsername()) {
            MqttCodecUtil.encodeUtf8String(payload, p.username());
        }

        // Password
        if (p.hasPassword()) {
            MqttCodecUtil.encodeBinaryData(payload, p.password());
        }

        return buildPacket(MqttPacketType.CONNECT.fixedHeaderByte(), varHeader, payload);
    }

    // ── CONNACK ───────────────────────────────────────────────────────────

    private static byte[] encodeConnAck(ConnAckPacket p, MqttVersion version) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Acknowledge Flags (bit 0 = Session Present)
        varHeader.write(p.sessionPresent() ? 0x01 : 0x00);

        if (version == MqttVersion.V5_0 && p.reasonCode() != null) {
            varHeader.write(p.reasonCode().code());
            encodeProperties(varHeader, p.properties());
        } else {
            varHeader.write(p.returnCode());
        }

        return buildPacket(MqttPacketType.CONNACK.fixedHeaderByte(), varHeader, null);
    }

    // ── PUBLISH ───────────────────────────────────────────────────────────

    private static byte[] encodePublish(PublishPacket p, MqttVersion version) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Topic Name
        MqttCodecUtil.encodeUtf8String(varHeader, p.topicName());

        // Packet Identifier (only for QoS > 0)
        if (p.qos() != MqttQoS.AT_MOST_ONCE) {
            MqttCodecUtil.encodeUnsignedShort(varHeader, p.packetId());
        }

        // v5.0 properties
        if (version == MqttVersion.V5_0) {
            encodeProperties(varHeader, p.properties());
        }

        // Fixed header byte with PUBLISH-specific flags
        MqttFixedHeader fh = MqttFixedHeader.forPublish(p.dup(), p.qos(), p.retain(), 0);
        int firstByte = fh.encodedFirstByte();

        // Remaining length = var header + payload
        byte[] varHeaderBytes = varHeader.toByteArray();
        int remainingLength = varHeaderBytes.length + p.payload().length;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(firstByte);
        MqttCodecUtil.encodeVariableByteInteger(out, remainingLength);
        out.write(varHeaderBytes);
        out.write(p.payload());
        return out.toByteArray();
    }

    // ── PUBACK / PUBREC / PUBREL / PUBCOMP ────────────────────────────────

    private static byte[] encodePubAckStyle(
        MqttPacketType type, int packetId,
        MqttReasonCode reasonCode, MqttProperties properties,
        MqttVersion version
    ) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Packet Identifier
        MqttCodecUtil.encodeUnsignedShort(varHeader, packetId);

        if (version == MqttVersion.V5_0 && reasonCode != null) {
            varHeader.write(reasonCode.code());
            // Only encode properties if they are present and non-empty
            if (properties != null && !properties.isEmpty()) {
                encodeProperties(varHeader, properties);
            } else {
                // If reason code is SUCCESS and no properties, we can omit both
                // But if we wrote the reason code, we should write empty properties
                // per spec: if there are properties, write property length 0
                MqttCodecUtil.encodeVariableByteInteger(varHeader, 0);
            }
        }

        return buildPacket(type.fixedHeaderByte(), varHeader, null);
    }

    // ── SUBSCRIBE ─────────────────────────────────────────────────────────

    private static byte[] encodeSubscribe(SubscribePacket p, MqttVersion version) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Packet Identifier
        MqttCodecUtil.encodeUnsignedShort(varHeader, p.packetId());

        // v5.0 properties
        if (version == MqttVersion.V5_0) {
            encodeProperties(varHeader, p.properties());
        }

        // Payload: topic filters + subscription options
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (SubscribePacket.Subscription sub : p.subscriptions()) {
            MqttCodecUtil.encodeUtf8String(payload, sub.topicFilter());
            if (version == MqttVersion.V5_0) {
                payload.write(sub.optionsByte());
            } else {
                // v3.1.1: just the QoS byte
                payload.write(sub.requestedQos().value());
            }
        }

        return buildPacket(MqttPacketType.SUBSCRIBE.fixedHeaderByte(), varHeader, payload);
    }

    // ── SUBACK ────────────────────────────────────────────────────────────

    private static byte[] encodeSubAck(SubAckPacket p, MqttVersion version) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Packet Identifier
        MqttCodecUtil.encodeUnsignedShort(varHeader, p.packetId());

        // v5.0 properties
        if (version == MqttVersion.V5_0) {
            encodeProperties(varHeader, p.properties());
        }

        // Payload: reason codes
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (int rc : p.reasonCodes()) {
            payload.write(rc);
        }

        return buildPacket(MqttPacketType.SUBACK.fixedHeaderByte(), varHeader, payload);
    }

    // ── UNSUBSCRIBE ───────────────────────────────────────────────────────

    private static byte[] encodeUnsubscribe(UnsubscribePacket p, MqttVersion version) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Packet Identifier
        MqttCodecUtil.encodeUnsignedShort(varHeader, p.packetId());

        // v5.0 properties
        if (version == MqttVersion.V5_0) {
            encodeProperties(varHeader, p.properties());
        }

        // Payload: topic filters
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (String tf : p.topicFilters()) {
            MqttCodecUtil.encodeUtf8String(payload, tf);
        }

        return buildPacket(MqttPacketType.UNSUBSCRIBE.fixedHeaderByte(), varHeader, payload);
    }

    // ── UNSUBACK ──────────────────────────────────────────────────────────

    private static byte[] encodeUnsubAck(UnsubAckPacket p, MqttVersion version) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();

        // Packet Identifier
        MqttCodecUtil.encodeUnsignedShort(varHeader, p.packetId());

        if (version == MqttVersion.V5_0) {
            encodeProperties(varHeader, p.properties());

            // v5.0 payload: reason codes
            if (p.reasonCodes() != null) {
                ByteArrayOutputStream payload = new ByteArrayOutputStream();
                for (int rc : p.reasonCodes()) {
                    payload.write(rc);
                }
                return buildPacket(MqttPacketType.UNSUBACK.fixedHeaderByte(), varHeader, payload);
            }
        }

        return buildPacket(MqttPacketType.UNSUBACK.fixedHeaderByte(), varHeader, null);
    }

    // ── DISCONNECT ────────────────────────────────────────────────────────

    private static byte[] encodeDisconnect(DisconnectPacket p, MqttVersion version) throws IOException {
        if (version == MqttVersion.V3_1_1) {
            return new byte[]{(byte) 0xE0, 0x00};
        }

        // v5.0: reason code + properties
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();
        MqttReasonCode rc = p.reasonCode();
        if (rc != null) {
            varHeader.write(rc.code());
            if (p.properties() != null && !p.properties().isEmpty()) {
                encodeProperties(varHeader, p.properties());
            } else if (varHeader.size() > 0) {
                // Write empty properties length
                MqttCodecUtil.encodeVariableByteInteger(varHeader, 0);
            }
        }

        return buildPacket(MqttPacketType.DISCONNECT.fixedHeaderByte(), varHeader, null);
    }

    // ── AUTH ──────────────────────────────────────────────────────────────

    private static byte[] encodeAuth(AuthPacket p) throws IOException {
        ByteArrayOutputStream varHeader = new ByteArrayOutputStream();
        varHeader.write(p.reasonCode().code());
        encodeProperties(varHeader, p.properties());
        return buildPacket(MqttPacketType.AUTH.fixedHeaderByte(), varHeader, null);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * Encodes properties to the output stream, writing an empty properties
     * length (0) if properties is {@code null} or empty.
     */
    private static void encodeProperties(OutputStream out, MqttProperties props) throws IOException {
        if (props != null && !props.isEmpty()) {
            props.encode(out);
        } else {
            // Empty properties: just the property length = 0
            MqttCodecUtil.encodeVariableByteInteger(out, 0);
        }
    }

    /**
     * Builds the complete packet: fixed header byte + remaining length VBI +
     * variable header bytes + payload bytes.
     */
    private static byte[] buildPacket(int firstByte, ByteArrayOutputStream varHeader,
                                       ByteArrayOutputStream payload) throws IOException {
        byte[] varHeaderBytes = varHeader.toByteArray();
        byte[] payloadBytes = (payload != null) ? payload.toByteArray() : new byte[0];
        int remainingLength = varHeaderBytes.length + payloadBytes.length;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(firstByte);
        MqttCodecUtil.encodeVariableByteInteger(out, remainingLength);
        out.write(varHeaderBytes);
        if (payloadBytes.length > 0) {
            out.write(payloadBytes);
        }
        return out.toByteArray();
    }
}
