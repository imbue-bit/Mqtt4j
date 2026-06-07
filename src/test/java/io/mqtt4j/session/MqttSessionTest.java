package io.mqtt4j.session;

import io.mqtt4j.message.MqttQoS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link MqttSession}, particularly topic matching.
 */
class MqttSessionTest {

    private MqttSession session;

    @BeforeEach
    void setUp() {
        session = new MqttSession("test-client", true);
    }

    // ── Subscription management ────────────────────────────────────────────

    @Test
    @DisplayName("Add and retrieve subscriptions")
    void testAddSubscription() {
        session.addSubscription("sensor/temp", MqttQoS.AT_LEAST_ONCE);
        session.addSubscription("device/#", MqttQoS.EXACTLY_ONCE);

        assertEquals(2, session.getSubscriptions().size());
        assertEquals(MqttQoS.AT_LEAST_ONCE, session.getSubscriptions().get("sensor/temp"));
        assertEquals(MqttQoS.EXACTLY_ONCE, session.getSubscriptions().get("device/#"));
    }

    @Test
    @DisplayName("Remove subscription")
    void testRemoveSubscription() {
        session.addSubscription("sensor/temp", MqttQoS.AT_LEAST_ONCE);
        session.removeSubscription("sensor/temp");

        assertEquals(0, session.getSubscriptions().size());
    }

    @Test
    @DisplayName("Clear resets session")
    void testClear() {
        session.addSubscription("sensor/temp", MqttQoS.AT_LEAST_ONCE);
        session.packetIdAllocator().allocate();

        session.clear();

        assertEquals(0, session.getSubscriptions().size());
    }

    // ── Topic matching ─────────────────────────────────────────────────────

    @ParameterizedTest(name = "Filter \"{0}\" matches topic \"{1}\" → {2}")
    @CsvSource({
        // Exact match
        "sensor/temp, sensor/temp, true",
        "sensor/temp, sensor/humidity, false",
        "a/b/c, a/b/c, true",
        "a/b/c, a/b/d, false",

        // Single-level wildcard (+)
        "sensor/+/temp, sensor/1/temp, true",
        "sensor/+/temp, sensor/2/temp, true",
        "sensor/+/temp, sensor/abc/temp, true",
        "sensor/+/temp, sensor/1/2/temp, false",
        "sensor/+/temp, sensor/temp, false",
        "+/temp, sensor/temp, true",
        "+/+/+, a/b/c, true",
        "+/+, a/b, true",
        "+, sensor, true",

        // Multi-level wildcard (#)
        "sensor/#, sensor, true",
        "sensor/#, sensor/temp, true",
        "sensor/#, sensor/temp/value, true",
        "sensor/#, sensor/a/b/c/d, true",
        "#, anything, true",
        "#, a/b/c, true",
        "#, '', true",

        // Combined wildcards
        "sensor/+/#, sensor/1, true",
        "sensor/+/#, sensor/1/temp, true",
        "sensor/+/#, sensor/1/temp/value, true",

        // Edge cases
        "sensor/temp, sensor/temp/, false",
        "/sensor/temp, /sensor/temp, true",
        "/sensor/temp, sensor/temp, false",
    })
    @DisplayName("Topic matching")
    void testTopicMatching(String filter, String topic, boolean expected) {
        assertEquals(expected, MqttSession.matchTopic(filter, topic),
            "matchTopic('" + filter + "', '" + topic + "') should be " + expected);
    }

    @Test
    @DisplayName("System topics not matched by leading # or +")
    void testSystemTopicProtection() {
        // $SYS topics should not be matched by leading wildcards
        assertFalse(MqttSession.matchTopic("#", "$SYS/broker/uptime"));
        assertFalse(MqttSession.matchTopic("+/broker/uptime", "$SYS/broker/uptime"));

        // But explicit subscription to $SYS should work
        assertTrue(MqttSession.matchTopic("$SYS/#", "$SYS/broker/uptime"));
        assertTrue(MqttSession.matchTopic("$SYS/broker/uptime", "$SYS/broker/uptime"));
    }
}
