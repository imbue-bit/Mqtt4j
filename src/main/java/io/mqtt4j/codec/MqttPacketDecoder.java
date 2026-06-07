package io.mqtt4j.codec;

import io.mqtt4j.MqttException;
import io.mqtt4j.MqttVersion;
import io.mqtt4j.message.MqttQoS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes MQTT packets from their wire format.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * InputStream in = ...; // network stream
 * MqttPacket packet = MqttPacketDecoder.decode(in, MqttVersion.V3_1_1);
 * switch (packet) {
 *     case ConnAckPacket ack -> handleConnAck(ack);
 *     case PublishPacket pub -> handlePublish(pub);
 *     // ...
 * }
 * }</pre>
 *
 * <p>The decoder reads the fixed header (first byte + remaining length VBI),
 * reads the remaining bytes into a buffer, then dispatches to a per-type
 * decoder.</p>
 */
public final class MqttPacketDecoder {

    private MqttPacketDecoder() {} // utility class

    /**
     * Decodes a single MQTT packet from the input stream.
     *
     * @param in      the input stream (e.g. network socket input)
     * @param version the expected MQTT protocol version
     * @return the decoded packet
     * @throws IOException              if an I/O error occurs
     * @throws MqttException.CodecException if the packet is malformed
     */
    public static MqttPacket decode(InputStream in, MqttVersion version) throws IOException {
        // Read fixed header: first byte
        int firstByte = MqttCodecUtil.readByte(in);
        // Read remaining length (VBI)
        int remainingLength = MqttCodecUtil.decodeVariableByteInteger(in);

        // Decode the fixed header
        MqttFixedHeader fixedHeader = MqttFixedHeader.decode(firstByte, remainingLength);

        // Read remaining bytes
        byte[] remainingBytes = MqttCodecUtil.readBytes(in, remainingLength);
        ByteArrayInputStream payload = new ByteArrayInputStream(remainingBytes);

        return decodeByType(fixedHeader, payload, remainingLength, version);
    }

    /**
     * Decodes a packet from an already-read byte buffer (fixed header already parsed).
     */
    private static MqttPacket decodeByType(MqttFixedHeader fh, ByteArrayInputStream in,
                                            int remainingLength, MqttVersion version) throws IOException {
        return switch (fh.packetType()) {
            case CONNECT     -> decodeConnect(in, remainingLength);
            case CONNACK     -> decodeConnAck(in, remainingLength, version);
            case PUBLISH     -> decodePublish(fh, in, remainingLength, version);
            case PUBACK      -> decodePubAck(in, remainingLength, version);
            case PUBREC      -> decodePubRec(in, remainingLength, version);
            case PUBREL      -> decodePubRel(in, remainingLength, version);
            case PUBCOMP     -> decodePubComp(in, remainingLength, version);
            case SUBSCRIBE   -> decodeSubscribe(in, remainingLength, version);
            case SUBACK      -> decodeSubAck(in, remainingLength, version);
            case UNSUBSCRIBE -> decodeUnsubscribe(in, remainingLength, version);
            case UNSUBACK    -> decodeUnsubAck(in, remainingLength, version);
            case PINGREQ     -> PingReqPacket.INSTANCE;
            case PINGRESP    -> PingRespPacket.INSTANCE;
            case DISCONNECT  -> decodeDisconnect(in, remainingLength, version);
            case AUTH        -> decodeAuth(in);
        };
    }

    // ── CONNECT ───────────────────────────────────────────────────────────

    private static ConnectPacket decodeConnect(ByteArrayInputStream in, int remainingLength) throws IOException {
        int startAvailable = in.available();

        // Protocol Name
        String protocolName = MqttCodecUtil.decodeUtf8String(in);
        if (!"MQTT".equals(protocolName)) {
            throw new MqttException.CodecException("Unsupported protocol name: " + protocolName);
        }

        // Protocol Level
        int protocolLevel = MqttCodecUtil.readByte(in);
        MqttVersion version = MqttVersion.fromProtocolLevel(protocolLevel);

        // Connect Flags
        int connectFlags = MqttCodecUtil.readByte(in);
        boolean hasUsername  = (connectFlags & 0x80) != 0;
        boolean hasPassword  = (connectFlags & 0x40) != 0;
        boolean willRetain   = (connectFlags & 0x20) != 0;
        int willQosValue     = (connectFlags >> 3) & 0x03;
        boolean hasWill      = (connectFlags & 0x04) != 0;
        boolean cleanSession = (connectFlags & 0x02) != 0;

        // Keep Alive
        int keepAlive = MqttCodecUtil.decodeUnsignedShort(in);

        // v5.0 connection properties
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0) {
            properties = MqttProperties.decode(in);
        }

        // ── Payload ────────────────────────────────────────────────────────
        // Client Identifier
        String clientId = MqttCodecUtil.decodeUtf8String(in);

        // Will
        String willTopic = null;
        byte[] willMessage = null;
        MqttQoS willQos = MqttQoS.AT_MOST_ONCE;
        MqttProperties willProperties = null;

        if (hasWill) {
            willQos = MqttQoS.valueOf(willQosValue);
            if (version == MqttVersion.V5_0) {
                willProperties = MqttProperties.decode(in);
            }
            willTopic = MqttCodecUtil.decodeUtf8String(in);
            willMessage = MqttCodecUtil.decodeBinaryData(in);
        }

        // Username
        String username = null;
        if (hasUsername) {
            username = MqttCodecUtil.decodeUtf8String(in);
        }

        // Password
        byte[] password = null;
        if (hasPassword) {
            password = MqttCodecUtil.decodeBinaryData(in);
        }

        return new ConnectPacket(version, cleanSession, keepAlive, clientId,
            willTopic, willMessage, willQos, willRetain,
            username, password, properties, willProperties);
    }

    // ── CONNACK ───────────────────────────────────────────────────────────

    private static ConnAckPacket decodeConnAck(ByteArrayInputStream in, int remainingLength,
                                                MqttVersion version) throws IOException {
        // Acknowledge Flags
        int ackFlags = MqttCodecUtil.readByte(in);
        boolean sessionPresent = (ackFlags & 0x01) != 0;

        // Return Code / Reason Code
        int returnCode = MqttCodecUtil.readByte(in);

        MqttReasonCode reasonCode = null;
        MqttProperties properties = null;

        if (version == MqttVersion.V5_0) {
            reasonCode = MqttReasonCode.fromCode(returnCode);
            if (in.available() > 0) {
                properties = MqttProperties.decode(in);
            }
        }

        return new ConnAckPacket(sessionPresent, returnCode, reasonCode, properties);
    }

    // ── PUBLISH ───────────────────────────────────────────────────────────

    private static PublishPacket decodePublish(MqttFixedHeader fh, ByteArrayInputStream in,
                                               int remainingLength, MqttVersion version) throws IOException {
        int startAvailable = in.available();

        // Topic Name
        String topicName = MqttCodecUtil.decodeUtf8String(in);

        // Packet Identifier (only for QoS > 0)
        int packetId = 0;
        if (fh.qos() != MqttQoS.AT_MOST_ONCE) {
            packetId = MqttCodecUtil.decodeUnsignedShort(in);
        }

        // v5.0 properties
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0) {
            properties = MqttProperties.decode(in);
        }

        // Payload: remaining bytes
        int consumed = startAvailable - in.available();
        int payloadLength = remainingLength - consumed;
        byte[] payload = new byte[0];
        if (payloadLength > 0) {
            payload = MqttCodecUtil.readBytes(in, payloadLength);
        }

        return new PublishPacket(fh.dup(), fh.qos(), fh.retain(), topicName, packetId, payload, properties);
    }

    // ── PUBACK / PUBREC / PUBREL / PUBCOMP ────────────────────────────────

    private static PubAckPacket decodePubAck(ByteArrayInputStream in, int remainingLength,
                                              MqttVersion version) throws IOException {
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);
        MqttReasonCode reasonCode = null;
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0 && remainingLength > 2) {
            reasonCode = MqttReasonCode.fromCode(MqttCodecUtil.readByte(in));
            if (remainingLength > 3) {
                properties = MqttProperties.decode(in);
            }
        }
        return new PubAckPacket(packetId, reasonCode, properties);
    }

    private static PubRecPacket decodePubRec(ByteArrayInputStream in, int remainingLength,
                                              MqttVersion version) throws IOException {
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);
        MqttReasonCode reasonCode = null;
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0 && remainingLength > 2) {
            reasonCode = MqttReasonCode.fromCode(MqttCodecUtil.readByte(in));
            if (remainingLength > 3) {
                properties = MqttProperties.decode(in);
            }
        }
        return new PubRecPacket(packetId, reasonCode, properties);
    }

    private static PubRelPacket decodePubRel(ByteArrayInputStream in, int remainingLength,
                                              MqttVersion version) throws IOException {
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);
        MqttReasonCode reasonCode = null;
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0 && remainingLength > 2) {
            reasonCode = MqttReasonCode.fromCode(MqttCodecUtil.readByte(in));
            if (remainingLength > 3) {
                properties = MqttProperties.decode(in);
            }
        }
        return new PubRelPacket(packetId, reasonCode, properties);
    }

    private static PubCompPacket decodePubComp(ByteArrayInputStream in, int remainingLength,
                                                MqttVersion version) throws IOException {
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);
        MqttReasonCode reasonCode = null;
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0 && remainingLength > 2) {
            reasonCode = MqttReasonCode.fromCode(MqttCodecUtil.readByte(in));
            if (remainingLength > 3) {
                properties = MqttProperties.decode(in);
            }
        }
        return new PubCompPacket(packetId, reasonCode, properties);
    }

    // ── SUBSCRIBE ─────────────────────────────────────────────────────────

    private static SubscribePacket decodeSubscribe(ByteArrayInputStream in, int remainingLength,
                                                    MqttVersion version) throws IOException {
        int startAvailable = in.available();

        // Packet Identifier
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);

        // v5.0 properties
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0) {
            properties = MqttProperties.decode(in);
        }

        // Payload: topic filters + subscription options
        List<SubscribePacket.Subscription> subscriptions = new ArrayList<>();
        while (in.available() > 0) {
            String topicFilter = MqttCodecUtil.decodeUtf8String(in);
            int optionsByte = MqttCodecUtil.readByte(in);

            MqttQoS qos = MqttQoS.valueOf(optionsByte & 0x03);
            boolean noLocal = false;
            boolean retainAsPublished = false;
            int retainHandling = 0;

            if (version == MqttVersion.V5_0) {
                noLocal = (optionsByte & 0x04) != 0;
                retainAsPublished = (optionsByte & 0x08) != 0;
                retainHandling = (optionsByte >> 4) & 0x03;
            }

            subscriptions.add(new SubscribePacket.Subscription(
                topicFilter, qos, noLocal, retainAsPublished, retainHandling));
        }

        return new SubscribePacket(packetId, subscriptions, properties);
    }

    // ── SUBACK ────────────────────────────────────────────────────────────

    private static SubAckPacket decodeSubAck(ByteArrayInputStream in, int remainingLength,
                                              MqttVersion version) throws IOException {
        // Packet Identifier
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);

        // v5.0 properties
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0) {
            properties = MqttProperties.decode(in);
        }

        // Payload: reason codes
        List<Integer> reasonCodes = new ArrayList<>();
        while (in.available() > 0) {
            reasonCodes.add(MqttCodecUtil.readByte(in));
        }

        return new SubAckPacket(packetId, reasonCodes, properties);
    }

    // ── UNSUBSCRIBE ───────────────────────────────────────────────────────

    private static UnsubscribePacket decodeUnsubscribe(ByteArrayInputStream in, int remainingLength,
                                                        MqttVersion version) throws IOException {
        // Packet Identifier
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);

        // v5.0 properties
        MqttProperties properties = null;
        if (version == MqttVersion.V5_0) {
            properties = MqttProperties.decode(in);
        }

        // Payload: topic filters
        List<String> topicFilters = new ArrayList<>();
        while (in.available() > 0) {
            topicFilters.add(MqttCodecUtil.decodeUtf8String(in));
        }

        return new UnsubscribePacket(packetId, topicFilters, properties);
    }

    // ── UNSUBACK ──────────────────────────────────────────────────────────

    private static UnsubAckPacket decodeUnsubAck(ByteArrayInputStream in, int remainingLength,
                                                  MqttVersion version) throws IOException {
        // Packet Identifier
        int packetId = MqttCodecUtil.decodeUnsignedShort(in);

        if (version == MqttVersion.V5_0 && in.available() > 0) {
            MqttProperties properties = MqttProperties.decode(in);

            // Payload: reason codes
            List<Integer> reasonCodes = new ArrayList<>();
            while (in.available() > 0) {
                reasonCodes.add(MqttCodecUtil.readByte(in));
            }

            return new UnsubAckPacket(packetId, reasonCodes, properties);
        }

        // v3.1.1: just the packet identifier
        return new UnsubAckPacket(packetId);
    }

    // ── DISCONNECT ────────────────────────────────────────────────────────

    private static DisconnectPacket decodeDisconnect(ByteArrayInputStream in, int remainingLength,
                                                      MqttVersion version) throws IOException {
        if (version == MqttVersion.V3_1_1 || remainingLength == 0) {
            return DisconnectPacket.V3_INSTANCE;
        }

        // v5.0: reason code + optional properties
        MqttReasonCode reasonCode = MqttReasonCode.fromCode(MqttCodecUtil.readByte(in));
        MqttProperties properties = null;
        if (in.available() > 0) {
            properties = MqttProperties.decode(in);
        }

        return new DisconnectPacket(reasonCode, properties);
    }

    // ── AUTH ──────────────────────────────────────────────────────────────

    private static AuthPacket decodeAuth(ByteArrayInputStream in) throws IOException {
        MqttReasonCode reasonCode = MqttReasonCode.fromCodeOrThrow(MqttCodecUtil.readByte(in));
        MqttProperties properties = MqttProperties.decode(in);
        return new AuthPacket(reasonCode, properties);
    }
}
