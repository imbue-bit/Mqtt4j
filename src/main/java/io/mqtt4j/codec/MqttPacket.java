package io.mqtt4j.codec;

/**
 * Sealed interface for all MQTT control packets.
 *
 * <p>This is the base type for the MQTT packet hierarchy, enabling
 * exhaustive pattern matching in {@code switch} expressions:</p>
 *
 * <pre>{@code
 * switch (packet) {
 *     case ConnectPacket p   -> handleConnect(p);
 *     case ConnAckPacket p   -> handleConnAck(p);
 *     case PublishPacket p   -> handlePublish(p);
 *     // ...
 * }
 * }</pre>
 */
public sealed interface MqttPacket
    permits ConnectPacket, ConnAckPacket,
            PublishPacket, PubAckPacket, PubRecPacket, PubRelPacket, PubCompPacket,
            SubscribePacket, SubAckPacket,
            UnsubscribePacket, UnsubAckPacket,
            PingReqPacket, PingRespPacket,
            DisconnectPacket, AuthPacket {

    /** Returns the packet type. */
    MqttPacketType type();
}
