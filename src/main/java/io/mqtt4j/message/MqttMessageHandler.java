package io.mqtt4j.message;

/**
 * Functional interface for receiving MQTT messages.
 *
 * <p>Implementations are invoked on a virtual thread for each incoming
 * PUBLISH message matching a subscribed topic filter.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * client.subscribe("sensor/+/data", MqttQoS.AT_LEAST_ONCE,
 *     (topic, message) -> {
 *         System.out.println("Received on " + topic + ": " + message.payloadAsString());
 *     });
 * }</pre>
 */
@FunctionalInterface
public interface MqttMessageHandler {

    /**
     * Called when a message is received on a subscribed topic.
     *
     * @param topic   the topic the message was published to
     * @param message the received message
     */
    void onMessage(String topic, MqttMessage message);
}
