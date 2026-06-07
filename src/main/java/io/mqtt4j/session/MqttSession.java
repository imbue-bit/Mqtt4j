package io.mqtt4j.session;

import io.mqtt4j.handler.InflightManager;
import io.mqtt4j.handler.PacketIdAllocator;
import io.mqtt4j.message.MqttQoS;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT session state container.
 *
 * <p>Holds all state associated with an MQTT client session:</p>
 * <ul>
 *   <li>Active subscriptions (topic filter → QoS)</li>
 *   <li>Packet ID allocator</li>
 *   <li>In-flight message tracker</li>
 *   <li>Session identity (client ID, clean-session flag)</li>
 * </ul>
 *
 * <p>For MQTT v3.1.1, a <em>clean session</em> ({@code cleanSession = true})
 * discards all state on connect. A <em>persistent session</em> preserves
 * subscriptions and in-flight messages across reconnections.</p>
 *
 * <h3>Topic matching</h3>
 * <p>The static utility method {@link #matchTopic(String, String)} implements
 * MQTT topic filter matching with wildcard support:</p>
 * <ul>
 *   <li>{@code +} — matches exactly one topic level</li>
 *   <li>{@code #} — matches zero or more remaining levels (must be last)</li>
 * </ul>
 *
 * @see PacketIdAllocator
 * @see InflightManager
 */
public final class MqttSession {

    private static final Logger LOG = Logger.getLogger(MqttSession.class.getName());

    private final ConcurrentHashMap<String, MqttQoS> subscriptions = new ConcurrentHashMap<>();
    private final PacketIdAllocator packetIdAllocator;
    private final InflightManager inflightManager;
    private final String clientId;
    private final boolean cleanSession;

    /**
     * Creates a new MQTT session.
     *
     * @param clientId     the MQTT client identifier
     * @param cleanSession {@code true} for a clean session (no state preserved)
     */
    public MqttSession(String clientId, boolean cleanSession) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.cleanSession = cleanSession;
        this.packetIdAllocator = new PacketIdAllocator();
        this.inflightManager = new InflightManager();
    }

    /**
     * Creates a clean session with the given client ID.
     *
     * @param clientId the MQTT client identifier
     */
    public MqttSession(String clientId) {
        this(clientId, true);
    }

    // ── Identity ──────────────────────────────────────────────────────────

    /**
     * Returns the MQTT client identifier.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * Returns whether this is a clean session.
     */
    public boolean isCleanSession() {
        return cleanSession;
    }

    // ── Subscriptions ─────────────────────────────────────────────────────

    /**
     * Adds or updates a subscription.
     *
     * @param topicFilter the MQTT topic filter (may contain {@code +} and {@code #})
     * @param qos         the maximum QoS level for the subscription
     * @throws NullPointerException if topicFilter or qos is null
     */
    public void addSubscription(String topicFilter, MqttQoS qos) {
        Objects.requireNonNull(topicFilter, "topicFilter");
        Objects.requireNonNull(qos, "qos");
        subscriptions.put(topicFilter, qos);
        LOG.log(Level.FINE, "Subscription added: {0} @ {1}", new Object[]{topicFilter, qos});
    }

    /**
     * Removes a subscription.
     *
     * @param topicFilter the topic filter to remove
     * @return the QoS that was associated with the filter, or {@code null} if not subscribed
     */
    public MqttQoS removeSubscription(String topicFilter) {
        MqttQoS removed = subscriptions.remove(topicFilter);
        if (removed != null) {
            LOG.log(Level.FINE, "Subscription removed: {0}", topicFilter);
        }
        return removed;
    }

    /**
     * Returns an unmodifiable view of the current subscriptions.
     *
     * @return map of topic filter → QoS
     */
    public Map<String, MqttQoS> getSubscriptions() {
        return Collections.unmodifiableMap(subscriptions);
    }

    /**
     * Returns the QoS for the given topic filter, or {@code null} if not subscribed.
     *
     * @param topicFilter the topic filter to look up
     * @return the QoS, or {@code null}
     */
    public MqttQoS getSubscriptionQoS(String topicFilter) {
        return subscriptions.get(topicFilter);
    }

    /**
     * Returns the number of active subscriptions.
     */
    public int subscriptionCount() {
        return subscriptions.size();
    }

    // ── Components ────────────────────────────────────────────────────────

    /**
     * Returns the packet ID allocator for this session.
     */
    public PacketIdAllocator packetIdAllocator() {
        return packetIdAllocator;
    }

    /**
     * Returns the in-flight message manager for this session.
     */
    public InflightManager inflightManager() {
        return inflightManager;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────

    /**
     * Clears all session state: subscriptions, packet IDs, and in-flight messages.
     *
     * <p>Called on clean session connect or explicit session reset.</p>
     */
    public void clear() {
        subscriptions.clear();
        packetIdAllocator.reset();
        inflightManager.clear();
        LOG.log(Level.FINE, "Session cleared: clientId={0}", clientId);
    }

    // ── Topic matching ────────────────────────────────────────────────────

    /**
     * Matches an MQTT topic name against a topic filter with wildcard support.
     *
     * <h4>Wildcard rules (MQTT spec §4.7):</h4>
     * <ul>
     *   <li>{@code +} (single-level wildcard) — matches exactly one topic level.
     *       <br>Example: {@code sensor/+/data} matches {@code sensor/1/data}
     *       but not {@code sensor/1/2/data}</li>
     *   <li>{@code #} (multi-level wildcard) — matches zero or more remaining levels.
     *       Must be the last character, preceded by {@code /} or alone.
     *       <br>Example: {@code sensor/#} matches {@code sensor}, {@code sensor/1},
     *       {@code sensor/1/data}</li>
     *   <li>No wildcards → exact string match</li>
     * </ul>
     *
     * <h4>Special cases:</h4>
     * <ul>
     *   <li>{@code #} alone matches all topics</li>
     *   <li>Topics starting with {@code $} (system topics) are NOT matched by
     *       filters starting with {@code #} or {@code +} — this matches the
     *       MQTT specification behavior</li>
     * </ul>
     *
     * @param topicFilter the topic filter (may contain {@code +} and {@code #})
     * @param topicName   the actual topic name to match against (no wildcards)
     * @return {@code true} if the topic name matches the filter
     * @throws NullPointerException if either argument is null
     */
    public static boolean matchTopic(String topicFilter, String topicName) {
        Objects.requireNonNull(topicFilter, "topicFilter");
        Objects.requireNonNull(topicName, "topicName");

        // System topics ($SYS etc.) are not matched by leading # or +
        if (topicName.startsWith("$") &&
                (topicFilter.startsWith("#") || topicFilter.startsWith("+"))) {
            return false;
        }

        // Exact match shortcut (no wildcards)
        if (!topicFilter.contains("+") && !topicFilter.contains("#")) {
            return topicFilter.equals(topicName);
        }

        String[] filterLevels = topicFilter.split("/", -1);
        String[] nameLevels = topicName.split("/", -1);

        int fi = 0; // filter level index
        int ni = 0; // name level index

        while (fi < filterLevels.length) {
            String filterLevel = filterLevels[fi];

            if ("#".equals(filterLevel)) {
                // '#' matches everything remaining (including nothing)
                return true;
            }

            if (ni >= nameLevels.length) {
                // More filter levels than topic levels — no match
                return false;
            }

            if ("+".equals(filterLevel)) {
                // '+' matches exactly this one level — continue
                fi++;
                ni++;
                continue;
            }

            // Literal match
            if (!filterLevel.equals(nameLevels[ni])) {
                return false;
            }

            fi++;
            ni++;
        }

        // Both must be fully consumed (unless filter ended with #, handled above)
        return ni == nameLevels.length;
    }

    /**
     * Finds the maximum QoS among all subscriptions whose filter matches the given topic.
     *
     * @param topicName the topic name to match against subscriptions
     * @return the maximum matching QoS, or {@code null} if no subscription matches
     */
    public MqttQoS findMatchingQoS(String topicName) {
        MqttQoS maxQoS = null;
        for (Map.Entry<String, MqttQoS> entry : subscriptions.entrySet()) {
            if (matchTopic(entry.getKey(), topicName)) {
                MqttQoS qos = entry.getValue();
                if (maxQoS == null || qos.value() > maxQoS.value()) {
                    maxQoS = qos;
                }
            }
        }
        return maxQoS;
    }

    @Override
    public String toString() {
        return "MqttSession[clientId='" + clientId + "'" +
                ", cleanSession=" + cleanSession +
                ", subscriptions=" + subscriptions.size() +
                ", inflight=" + inflightManager.size() + "]";
    }
}
