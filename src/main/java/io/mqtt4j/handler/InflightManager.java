package io.mqtt4j.handler;

import io.mqtt4j.codec.MqttPacket;
import io.mqtt4j.message.MqttQoS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages in-flight QoS 1 and QoS 2 PUBLISH messages.
 *
 * <p>For each outbound PUBLISH with QoS &gt; 0, an {@link InflightMessage} is tracked
 * until the full QoS handshake completes. Each in-flight message carries a
 * {@link CompletableFuture} that completes when the flow finishes successfully,
 * or completes exceptionally on error/timeout.</p>
 *
 * <h3>QoS 1 flow:</h3>
 * <ol>
 *   <li>PUBLISH sent → state = {@link InflightState#PENDING_PUBACK}</li>
 *   <li>PUBACK received → future completes, message removed</li>
 * </ol>
 *
 * <h3>QoS 2 flow:</h3>
 * <ol>
 *   <li>PUBLISH sent → state = {@link InflightState#PENDING_PUBREC}</li>
 *   <li>PUBREC received → state = {@link InflightState#PENDING_PUBREL}</li>
 *   <li>PUBREL sent → state = {@link InflightState#PENDING_PUBCOMP}</li>
 *   <li>PUBCOMP received → future completes, message removed</li>
 * </ol>
 *
 * <p>Thread-safe: uses a {@link ConcurrentHashMap} for the in-flight registry.</p>
 */
public final class InflightManager {

    private static final Logger LOG = Logger.getLogger(InflightManager.class.getName());

    // ── Inner types ────────────────────────────────────────────────────────

    /**
     * State of an in-flight QoS message within the MQTT handshake.
     */
    public enum InflightState {
        /** QoS 1: waiting for PUBACK. */
        PENDING_PUBACK,
        /** QoS 2: waiting for PUBREC (first step). */
        PENDING_PUBREC,
        /** QoS 2: PUBREC received, waiting to send / sent PUBREL. */
        PENDING_PUBREL,
        /** QoS 2: PUBREL sent, waiting for PUBCOMP. */
        PENDING_PUBCOMP
    }

    /**
     * Represents a single in-flight PUBLISH message and its QoS handshake state.
     *
     * @param packetId   the MQTT Packet Identifier
     * @param packet     the original PUBLISH packet (for retransmission)
     * @param qos        the QoS level
     * @param state      the current handshake state
     * @param sentTimeMs the timestamp (epoch millis) when the message was sent
     * @param retryCount the number of retransmission attempts made so far
     * @param future     the CompletableFuture that completes when the flow finishes
     */
    public record InflightMessage(
            int packetId,
            MqttPacket packet,
            MqttQoS qos,
            InflightState state,
            long sentTimeMs,
            int retryCount,
            CompletableFuture<Void> future
    ) {
        /**
         * Returns a copy of this message with an updated state.
         */
        public InflightMessage withState(InflightState newState) {
            return new InflightMessage(packetId, packet, qos, newState,
                    sentTimeMs, retryCount, future);
        }

        /**
         * Returns a copy of this message with an incremented retry count
         * and updated sent time.
         */
        public InflightMessage withRetry() {
            return new InflightMessage(packetId, packet, qos, state,
                    System.currentTimeMillis(), retryCount + 1, future);
        }
    }

    // ── Fields ─────────────────────────────────────────────────────────────

    private final ConcurrentHashMap<Integer, InflightMessage> inflight = new ConcurrentHashMap<>();

    // ── Tracking ───────────────────────────────────────────────────────────

    /**
     * Registers a new in-flight message for tracking.
     *
     * <p>For QoS 1, the initial state is {@link InflightState#PENDING_PUBACK}.
     * For QoS 2, the initial state is {@link InflightState#PENDING_PUBREC}.</p>
     *
     * @param packetId the Packet Identifier
     * @param packet   the PUBLISH packet
     * @param qos      the QoS level (must be 1 or 2)
     * @param future   the CompletableFuture to complete when the flow finishes
     * @throws IllegalArgumentException if QoS is 0 or the packet ID is already tracked
     */
    public void track(int packetId, MqttPacket packet, MqttQoS qos,
                      CompletableFuture<Void> future) {
        if (qos == MqttQoS.AT_MOST_ONCE) {
            throw new IllegalArgumentException("QoS 0 messages should not be tracked");
        }

        InflightState initialState = (qos == MqttQoS.AT_LEAST_ONCE)
                ? InflightState.PENDING_PUBACK
                : InflightState.PENDING_PUBREC;

        InflightMessage msg = new InflightMessage(
                packetId, packet, qos, initialState,
                System.currentTimeMillis(), 0, future);

        InflightMessage existing = inflight.putIfAbsent(packetId, msg);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Packet ID " + packetId + " is already in-flight");
        }

        LOG.log(Level.FINE, "Tracking in-flight message: packetId={0}, qos={1}, state={2}",
                new Object[]{packetId, qos, initialState});
    }

    // ── QoS 1 handling ────────────────────────────────────────────────────

    /**
     * Handles receipt of a PUBACK (QoS 1 completion).
     *
     * <p>Completes the associated future and removes the message from tracking.</p>
     *
     * @param packetId the Packet Identifier from the PUBACK
     * @return {@code true} if the message was found and completed
     */
    public boolean handlePubAck(int packetId) {
        InflightMessage msg = inflight.remove(packetId);
        if (msg == null) {
            LOG.log(Level.WARNING, "Received PUBACK for unknown packetId={0}", packetId);
            return false;
        }
        if (msg.state() != InflightState.PENDING_PUBACK) {
            LOG.log(Level.WARNING,
                    "Received PUBACK for packetId={0} in unexpected state {1}",
                    new Object[]{packetId, msg.state()});
        }
        msg.future().complete(null);
        LOG.log(Level.FINE, "QoS 1 complete: packetId={0}", packetId);
        return true;
    }

    // ── QoS 2 handling ────────────────────────────────────────────────────

    /**
     * Handles receipt of a PUBREC (QoS 2, step 2).
     *
     * <p>Transitions the message state to {@link InflightState#PENDING_PUBREL}.
     * The caller is expected to send a PUBREL in response.</p>
     *
     * @param packetId the Packet Identifier from the PUBREC
     * @return {@code true} if the message was found and transitioned
     */
    public boolean handlePubRec(int packetId) {
        InflightMessage msg = inflight.get(packetId);
        if (msg == null) {
            LOG.log(Level.WARNING, "Received PUBREC for unknown packetId={0}", packetId);
            return false;
        }
        if (msg.state() != InflightState.PENDING_PUBREC) {
            LOG.log(Level.WARNING,
                    "Received PUBREC for packetId={0} in unexpected state {1}",
                    new Object[]{packetId, msg.state()});
        }
        inflight.replace(packetId, msg.withState(InflightState.PENDING_PUBREL));
        LOG.log(Level.FINE, "QoS 2 PUBREC received: packetId={0} → PENDING_PUBREL", packetId);
        return true;
    }

    /**
     * Marks a message as having sent PUBREL (QoS 2, step 3).
     *
     * <p>Transitions the message state to {@link InflightState#PENDING_PUBCOMP}.</p>
     *
     * @param packetId the Packet Identifier
     * @return {@code true} if the message was found and transitioned
     */
    public boolean handlePubRelSent(int packetId) {
        InflightMessage msg = inflight.get(packetId);
        if (msg == null) {
            return false;
        }
        inflight.replace(packetId, msg.withState(InflightState.PENDING_PUBCOMP));
        LOG.log(Level.FINE, "QoS 2 PUBREL sent: packetId={0} → PENDING_PUBCOMP", packetId);
        return true;
    }

    /**
     * Handles receipt of a PUBCOMP (QoS 2 completion, step 4).
     *
     * <p>Completes the associated future and removes the message from tracking.</p>
     *
     * @param packetId the Packet Identifier from the PUBCOMP
     * @return {@code true} if the message was found and completed
     */
    public boolean handlePubComp(int packetId) {
        InflightMessage msg = inflight.remove(packetId);
        if (msg == null) {
            LOG.log(Level.WARNING, "Received PUBCOMP for unknown packetId={0}", packetId);
            return false;
        }
        if (msg.state() != InflightState.PENDING_PUBCOMP) {
            LOG.log(Level.WARNING,
                    "Received PUBCOMP for packetId={0} in unexpected state {1}",
                    new Object[]{packetId, msg.state()});
        }
        msg.future().complete(null);
        LOG.log(Level.FINE, "QoS 2 complete: packetId={0}", packetId);
        return true;
    }

    // ── Retry support ─────────────────────────────────────────────────────

    /**
     * Returns a list of in-flight messages that should be retried.
     *
     * <p>A message is retryable if:</p>
     * <ul>
     *   <li>It has been waiting longer than {@code timeoutMs} since its last send</li>
     *   <li>Its retry count is less than {@code maxRetries}</li>
     * </ul>
     *
     * @param maxRetries maximum number of retry attempts allowed
     * @param timeoutMs  timeout in milliseconds after which a message is eligible for retry
     * @return unmodifiable list of retryable messages
     */
    public List<InflightMessage> getRetryable(int maxRetries, long timeoutMs) {
        long now = System.currentTimeMillis();
        List<InflightMessage> retryable = new ArrayList<>();
        for (InflightMessage msg : inflight.values()) {
            if ((now - msg.sentTimeMs()) >= timeoutMs && msg.retryCount() < maxRetries) {
                retryable.add(msg);
            }
        }
        return Collections.unmodifiableList(retryable);
    }

    /**
     * Marks a message as having been retried (increments retry count, updates timestamp).
     *
     * @param packetId the Packet Identifier
     * @return {@code true} if the message was found and updated
     */
    public boolean markRetried(int packetId) {
        InflightMessage msg = inflight.get(packetId);
        if (msg == null) {
            return false;
        }
        inflight.replace(packetId, msg.withRetry());
        return true;
    }

    // ── Queries ────────────────────────────────────────────────────────────

    /**
     * Returns the in-flight message for the given packet ID, or {@code null}.
     *
     * @param packetId the Packet Identifier
     * @return the in-flight message, or {@code null} if not tracked
     */
    public InflightMessage get(int packetId) {
        return inflight.get(packetId);
    }

    /**
     * Removes an in-flight message from tracking.
     *
     * @param packetId the Packet Identifier
     * @return the removed message, or {@code null} if not tracked
     */
    public InflightMessage remove(int packetId) {
        return inflight.remove(packetId);
    }

    /**
     * Returns the number of messages currently in flight.
     *
     * @return the count of tracked messages
     */
    public int size() {
        return inflight.size();
    }

    /**
     * Returns {@code true} if there are no messages in flight.
     */
    public boolean isEmpty() {
        return inflight.isEmpty();
    }

    /**
     * Clears all in-flight messages.
     *
     * <p>Any pending futures are completed exceptionally with the given cause.
     * If {@code cause} is {@code null}, futures are cancelled.</p>
     *
     * @param cause the exception to complete futures with, or {@code null} to cancel
     */
    public void clear(Throwable cause) {
        for (InflightMessage msg : inflight.values()) {
            if (cause != null) {
                msg.future().completeExceptionally(cause);
            } else {
                msg.future().cancel(false);
            }
        }
        inflight.clear();
        LOG.log(Level.FINE, "Cleared all in-flight messages");
    }

    /**
     * Clears all in-flight messages, cancelling their futures.
     */
    public void clear() {
        clear(null);
    }

    @Override
    public String toString() {
        return "InflightManager[size=" + inflight.size() + "]";
    }
}
