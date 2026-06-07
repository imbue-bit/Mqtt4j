package io.mqtt4j.codec;

import io.mqtt4j.MqttVersion;
import io.mqtt4j.message.MqttQoS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link MqttPacketEncoder} and {@link MqttPacketDecoder} —
 * encoding packets to bytes and decoding them back.
 */
class MqttPacketCodecTest {

    // ── CONNECT / CONNACK round-trip ───────────────────────────────────────

    @Test
    @DisplayName("CONNECT v3.1.1 round-trip")
    void testConnectV311RoundTrip() throws IOException {
        ConnectPacket original = new ConnectPacket(
            MqttVersion.V3_1_1, true, 60, "test-client",
            null, null, MqttQoS.AT_MOST_ONCE, false,
            "user", "pass".getBytes(), null, null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        // First byte: type=1 (CONNECT), flags=0 → 0x10
        assertEquals(0x10, encoded[0] & 0xFF);

        // Decode
        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(ConnectPacket.class, decoded);
        ConnectPacket connPkt = (ConnectPacket) decoded;
        assertEquals("test-client", connPkt.clientId());
        assertEquals(60, connPkt.keepAliveSeconds());
        assertTrue(connPkt.cleanSession());
        assertEquals("user", connPkt.username());
    }

    @Test
    @DisplayName("CONNECT with LWT round-trip")
    void testConnectWithWillRoundTrip() throws IOException {
        ConnectPacket original = new ConnectPacket(
            MqttVersion.V3_1_1, true, 30, "will-client",
            "device/status", "offline".getBytes(),
            MqttQoS.AT_LEAST_ONCE, true,
            null, null, null, null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);
        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(ConnectPacket.class, decoded);
        ConnectPacket connPkt = (ConnectPacket) decoded;
        assertEquals("will-client", connPkt.clientId());
        assertEquals("device/status", connPkt.willTopic());
        assertArrayEquals("offline".getBytes(), connPkt.willMessage());
        assertEquals(MqttQoS.AT_LEAST_ONCE, connPkt.willQos());
        assertTrue(connPkt.willRetain());
    }

    // ── PUBLISH round-trip ─────────────────────────────────────────────────

    @Test
    @DisplayName("PUBLISH QoS 0 round-trip")
    void testPublishQos0RoundTrip() throws IOException {
        PublishPacket original = new PublishPacket(
            false, MqttQoS.AT_MOST_ONCE, false,
            "sensor/temp", 0, "26.5".getBytes(), null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);

        // First byte: type=3 (PUBLISH), DUP=0, QoS=0, Retain=0 → 0x30
        assertEquals(0x30, encoded[0] & 0xFF);

        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(PublishPacket.class, decoded);
        PublishPacket pubPkt = (PublishPacket) decoded;
        assertEquals("sensor/temp", pubPkt.topicName());
        assertEquals(MqttQoS.AT_MOST_ONCE, pubPkt.qos());
        assertArrayEquals("26.5".getBytes(), pubPkt.payload());
        assertFalse(pubPkt.dup());
        assertFalse(pubPkt.retain());
    }

    @Test
    @DisplayName("PUBLISH QoS 1 with retain round-trip")
    void testPublishQos1RetainRoundTrip() throws IOException {
        PublishPacket original = new PublishPacket(
            false, MqttQoS.AT_LEAST_ONCE, true,
            "device/status", 42, "online".getBytes(), null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);

        // First byte: type=3, DUP=0, QoS=1, Retain=1 → 0x33
        assertEquals(0x33, encoded[0] & 0xFF);

        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(PublishPacket.class, decoded);
        PublishPacket pubPkt = (PublishPacket) decoded;
        assertEquals(42, pubPkt.packetId());
        assertEquals(MqttQoS.AT_LEAST_ONCE, pubPkt.qos());
        assertTrue(pubPkt.retain());
    }

    @Test
    @DisplayName("PUBLISH QoS 2 round-trip")
    void testPublishQos2RoundTrip() throws IOException {
        PublishPacket original = new PublishPacket(
            true, MqttQoS.EXACTLY_ONCE, false,
            "cmd/execute", 1000, "reboot".getBytes(), null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);

        // First byte: type=3, DUP=1, QoS=2, Retain=0 → 0x3C
        assertEquals(0x3C, encoded[0] & 0xFF);

        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(PublishPacket.class, decoded);
        PublishPacket pubPkt = (PublishPacket) decoded;
        assertEquals(1000, pubPkt.packetId());
        assertTrue(pubPkt.dup());
        assertEquals(MqttQoS.EXACTLY_ONCE, pubPkt.qos());
    }

    // ── PUBACK round-trip ──────────────────────────────────────────────────

    @Test
    @DisplayName("PUBACK round-trip")
    void testPubAckRoundTrip() throws IOException {
        PubAckPacket original = new PubAckPacket(123, null, null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);
        assertEquals(0x40, encoded[0] & 0xFF);
        assertEquals(4, encoded.length); // fixed header (2) + packet id (2)

        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(PubAckPacket.class, decoded);
        assertEquals(123, ((PubAckPacket) decoded).packetId());
    }

    // ── SUBSCRIBE round-trip ───────────────────────────────────────────────

    @Test
    @DisplayName("SUBSCRIBE round-trip")
    void testSubscribeRoundTrip() throws IOException {
        SubscribePacket original = new SubscribePacket(
            256,
            List.of(
                SubscribePacket.Subscription.of("sensor/+/temp", MqttQoS.AT_LEAST_ONCE),
                SubscribePacket.Subscription.of("device/#", MqttQoS.EXACTLY_ONCE)
            ),
            null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);

        // First byte: type=8, flags=0010 → 0x82
        assertEquals(0x82, encoded[0] & 0xFF);

        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(SubscribePacket.class, decoded);
        SubscribePacket subPkt = (SubscribePacket) decoded;
        assertEquals(256, subPkt.packetId());
        assertEquals(2, subPkt.subscriptions().size());
        assertEquals("sensor/+/temp", subPkt.subscriptions().get(0).topicFilter());
        assertEquals(MqttQoS.AT_LEAST_ONCE, subPkt.subscriptions().get(0).requestedQos());
        assertEquals("device/#", subPkt.subscriptions().get(1).topicFilter());
        assertEquals(MqttQoS.EXACTLY_ONCE, subPkt.subscriptions().get(1).requestedQos());
    }

    // ── UNSUBSCRIBE round-trip ─────────────────────────────────────────────

    @Test
    @DisplayName("UNSUBSCRIBE round-trip")
    void testUnsubscribeRoundTrip() throws IOException {
        UnsubscribePacket original = new UnsubscribePacket(
            500, List.of("sensor/temp", "device/status"), null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);

        // First byte: type=10, flags=0010 → 0xA2
        assertEquals(0xA2, encoded[0] & 0xFF);

        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(UnsubscribePacket.class, decoded);
        UnsubscribePacket unsubPkt = (UnsubscribePacket) decoded;
        assertEquals(500, unsubPkt.packetId());
        assertEquals(List.of("sensor/temp", "device/status"), unsubPkt.topicFilters());
    }

    // ── PINGREQ / PINGRESP ─────────────────────────────────────────────────

    @Test
    @DisplayName("PINGREQ encoding")
    void testPingReqEncoding() throws IOException {
        byte[] encoded = MqttPacketEncoder.encode(PingReqPacket.INSTANCE, MqttVersion.V3_1_1);
        assertEquals(2, encoded.length);
        assertEquals(0xC0, encoded[0] & 0xFF);
        assertEquals(0x00, encoded[1] & 0xFF);
    }

    @Test
    @DisplayName("PINGRESP encoding")
    void testPingRespEncoding() throws IOException {
        byte[] encoded = MqttPacketEncoder.encode(PingRespPacket.INSTANCE, MqttVersion.V3_1_1);
        assertEquals(2, encoded.length);
        assertEquals(0xD0, encoded[0] & 0xFF);
        assertEquals(0x00, encoded[1] & 0xFF);
    }

    @Test
    @DisplayName("PINGREQ decode")
    void testPingReqDecode() throws IOException {
        byte[] data = {(byte) 0xC0, 0x00};
        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(data), MqttVersion.V3_1_1);
        assertInstanceOf(PingReqPacket.class, decoded);
    }

    @Test
    @DisplayName("PINGRESP decode")
    void testPingRespDecode() throws IOException {
        byte[] data = {(byte) 0xD0, 0x00};
        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(data), MqttVersion.V3_1_1);
        assertInstanceOf(PingRespPacket.class, decoded);
    }

    // ── DISCONNECT ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("DISCONNECT v3.1.1 encoding")
    void testDisconnectV311Encoding() throws IOException {
        byte[] encoded = MqttPacketEncoder.encode(
            DisconnectPacket.V3_INSTANCE, MqttVersion.V3_1_1);
        assertEquals(2, encoded.length);
        assertEquals(0xE0, encoded[0] & 0xFF);
        assertEquals(0x00, encoded[1] & 0xFF);
    }

    // ── Empty payload edge case ────────────────────────────────────────────

    @Test
    @DisplayName("PUBLISH with empty payload")
    void testPublishEmptyPayload() throws IOException {
        PublishPacket original = new PublishPacket(
            false, MqttQoS.AT_MOST_ONCE, false,
            "test/topic", 0, new byte[0], null);

        byte[] encoded = MqttPacketEncoder.encode(original, MqttVersion.V3_1_1);
        MqttPacket decoded = MqttPacketDecoder.decode(
            new ByteArrayInputStream(encoded), MqttVersion.V3_1_1);

        assertInstanceOf(PublishPacket.class, decoded);
        assertEquals(0, ((PublishPacket) decoded).payload().length);
    }
}
