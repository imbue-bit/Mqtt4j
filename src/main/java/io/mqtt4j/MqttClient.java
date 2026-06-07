package io.mqtt4j;

import io.mqtt4j.codec.*;
import io.mqtt4j.handler.InflightManager;
import io.mqtt4j.handler.KeepAliveManager;
import io.mqtt4j.handler.ReconnectManager;
import io.mqtt4j.message.MqttMessage;
import io.mqtt4j.message.MqttMessageHandler;
import io.mqtt4j.message.MqttQoS;
import io.mqtt4j.session.MqttSession;
import io.mqtt4j.transport.MqttTransport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lightweight MQTT client optimized for Java 21 virtual threads.
 *
 * <p>Supports MQTT v3.1.1 and v5.0, with zero runtime dependencies.
 * Each client instance manages a single connection to an MQTT broker.</p>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * var client = new MqttClient(MqttClientConfig.builder()
 *     .host("broker.emqx.io")
 *     .clientId("my-device")
 *     .build());
 *
 * client.connect();
 * client.subscribe("sensor/data", MqttQoS.AT_LEAST_ONCE,
 *     (topic, msg) -> System.out.println(msg.payloadAsString()));
 * client.publish("sensor/data", "Hello MQTT!".getBytes(), MqttQoS.AT_MOST_ONCE, false);
 * client.disconnect();
 * }</pre>
 *
 * <h2>Virtual Thread High-Concurrency</h2>
 * <pre>{@code
 * try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
 *     for (int i = 0; i < 10_000; i++) {
 *         final int id = i;
 *         executor.submit(() -> {
 *             var client = new MqttClient(MqttClientConfig.builder()
 *                 .host("broker.emqx.io")
 *                 .clientId("vt-" + id)
 *                 .build());
 *             client.connect();
 *             client.subscribe("device/" + id + "/data", MqttQoS.AT_LEAST_ONCE,
 *                 (topic, msg) -> System.out.println("Device " + id + ": " + msg.payloadAsString()));
 *         });
 *     }
 * }
 * }</pre>
 */
public class MqttClient implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(MqttClient.class.getName());

    private final MqttClientConfig config;
    private final MqttSession session;
    private final ReentrantLock writeLock = new ReentrantLock();

    // ── Connection state ───────────────────────────────────────────────────
    private volatile MqttTransport transport;
    private volatile OutputStream outputStream;
    private volatile Thread readLoopThread;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // ── Handlers ───────────────────────────────────────────────────────────
    private volatile KeepAliveManager keepAliveManager;
    private volatile ReconnectManager reconnectManager;

    // ── Subscriptions ──────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, SubscriptionEntry> subscriptionHandlers = new ConcurrentHashMap<>();

    private record SubscriptionEntry(MqttQoS qos, MqttMessageHandler handler) {}

    // ── Callbacks ──────────────────────────────────────────────────────────
    private final List<Consumer<Throwable>> connectionLostCallbacks = new CopyOnWriteArrayList<>();
    private final List<Runnable> connectedCallbacks = new CopyOnWriteArrayList<>();

    // ── Message dispatch executor (virtual threads) ────────────────────────
    private final ExecutorService messageExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // ── Connect future ─────────────────────────────────────────────────────
    private volatile CompletableFuture<Void> connectFuture;

    /**
     * Creates a new MQTT client with the given configuration.
     *
     * @param config the client configuration
     */
    public MqttClient(MqttClientConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.session = new MqttSession(config.clientId(), config.cleanSession());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Connection Management
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Connects to the MQTT broker (blocking).
     *
     * @throws MqttException.ConnectionException if the connection fails
     */
    public void connect() {
        try {
            connectAsync().get(config.connectTimeoutMs() + 5000, TimeUnit.MILLISECONDS);
        } catch (MqttException e) {
            throw e;
        } catch (Exception e) {
            throw new MqttException.ConnectionException("Connect failed", e);
        }
    }

    /**
     * Connects to the MQTT broker asynchronously.
     *
     * @return a future that completes when the CONNACK is received
     */
    public CompletableFuture<Void> connectAsync() {
        if (closed.get()) {
            throw new MqttException("Client has been closed");
        }
        if (connected.get()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!connecting.compareAndSet(false, true)) {
            return connectFuture != null ? connectFuture : CompletableFuture.completedFuture(null);
        }

        connectFuture = new CompletableFuture<>();

        Thread.ofVirtual().name("mqtt4j-connect-" + config.clientId()).start(() -> {
            try {
                doConnect();
            } catch (Exception e) {
                connecting.set(false);
                connectFuture.completeExceptionally(
                    new MqttException.ConnectionException("Connect failed: " + e.getMessage(), e));
            }
        });

        return connectFuture;
    }

    private void doConnect() throws IOException {
        LOG.info(() -> "Connecting to " + config.host() + ":" + config.port() +
                       " [" + config.version() + ", clientId=" + config.clientId() + "]");

        // 1. Establish transport
        transport = config.createTransport();
        transport.connect(config.host(), config.port(), config.connectTimeoutMs());
        outputStream = transport.getOutputStream();

        // 2. Start read loop (before sending CONNECT, so we can receive CONNACK)
        startReadLoop();

        // 3. Send CONNECT packet
        ConnectPacket connectPacket = new ConnectPacket(
            config.version(),
            config.cleanSession(),
            config.keepAliveSeconds(),
            config.clientId(),
            config.willTopic(),
            config.willMessage(),
            config.willQos() != null ? config.willQos() : MqttQoS.AT_MOST_ONCE,
            config.willRetain(),
            config.username(),
            config.password(),
            null,  // connect properties (v5.0, can be extended)
            null   // will properties (v5.0, can be extended)
        );
        sendPacket(connectPacket);

        LOG.fine("CONNECT packet sent, waiting for CONNACK...");
    }

    /**
     * Disconnects from the MQTT broker gracefully.
     */
    public void disconnect() {
        if (!connected.compareAndSet(true, false)) {
            return;
        }
        LOG.info(() -> "Disconnecting from " + config.host() + ":" + config.port());

        try {
            // Stop keep-alive
            if (keepAliveManager != null) {
                keepAliveManager.stop();
            }

            // Stop auto-reconnect
            if (reconnectManager != null) {
                reconnectManager.stop();
            }

            // Send DISCONNECT packet
            DisconnectPacket disconnectPacket;
            if (config.version() == MqttVersion.V5_0) {
                disconnectPacket = new DisconnectPacket(MqttReasonCode.NORMAL_DISCONNECTION, null);
            } else {
                disconnectPacket = DisconnectPacket.V3_INSTANCE;
            }
            try {
                sendPacket(disconnectPacket);
            } catch (Exception e) {
                LOG.log(Level.FINE, "Error sending DISCONNECT", e);
            }

            // Close transport
            closeTransport();
        } finally {
            session.inflightManager().clear(
                new MqttException.ConnectionLostException("Client disconnected"));
        }
    }

    /**
     * Returns {@code true} if the client is currently connected.
     */
    public boolean isConnected() {
        return connected.get() && transport != null && transport.isConnected();
    }

    /**
     * Registers a callback invoked when the connection is lost.
     */
    public MqttClient onConnectionLost(Consumer<Throwable> callback) {
        connectionLostCallbacks.add(Objects.requireNonNull(callback));
        return this;
    }

    /**
     * Registers a callback invoked when the connection is established.
     */
    public MqttClient onConnected(Runnable callback) {
        connectedCallbacks.add(Objects.requireNonNull(callback));
        return this;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Publish
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Publishes a message to a topic (blocking for QoS 1/2 acknowledgment).
     *
     * @param topic   the topic name
     * @param payload the message payload
     * @param qos     the quality of service level
     * @param retain  whether to retain the message
     */
    public void publish(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        try {
            publishAsync(topic, payload, qos, retain)
                .get(config.publishTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (MqttException e) {
            throw e;
        } catch (Exception e) {
            throw new MqttException("Publish failed", e);
        }
    }

    /**
     * Publishes a string message with QoS 0 and no retain.
     */
    public void publish(String topic, String payload) {
        publish(topic, payload.getBytes(StandardCharsets.UTF_8), MqttQoS.AT_MOST_ONCE, false);
    }

    /**
     * Publishes a message asynchronously.
     *
     * @return a future that completes when the QoS flow finishes
     *         (immediately for QoS 0, on PUBACK for QoS 1, on PUBCOMP for QoS 2)
     */
    public CompletableFuture<Void> publishAsync(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        ensureConnected();
        Objects.requireNonNull(topic, "topic");

        int packetId = 0;
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (qos != MqttQoS.AT_MOST_ONCE) {
            packetId = session.packetIdAllocator().allocate();
        }

        PublishPacket packet = new PublishPacket(
            false, qos, retain, topic, packetId,
            payload != null ? payload : new byte[0], null);

        if (qos != MqttQoS.AT_MOST_ONCE) {
            session.inflightManager().track(packetId, packet, qos, future);
        }

        try {
            sendPacket(packet);
            if (qos == MqttQoS.AT_MOST_ONCE) {
                future.complete(null);
            }
        } catch (Exception e) {
            if (qos != MqttQoS.AT_MOST_ONCE) {
                session.packetIdAllocator().release(packetId);
                session.inflightManager().remove(packetId);
            }
            future.completeExceptionally(new MqttException("Publish failed", e));
        }

        return future;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Subscribe / Unsubscribe
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Subscribes to a topic filter.
     *
     * @param topicFilter the topic filter (supports + and # wildcards)
     * @param qos         the maximum QoS level
     * @param handler     callback for received messages
     */
    public void subscribe(String topicFilter, MqttQoS qos, MqttMessageHandler handler) {
        ensureConnected();
        Objects.requireNonNull(topicFilter, "topicFilter");
        Objects.requireNonNull(handler, "handler");

        int packetId = session.packetIdAllocator().allocate();

        SubscribePacket.Subscription sub = SubscribePacket.Subscription.of(topicFilter, qos);
        SubscribePacket packet = new SubscribePacket(packetId, List.of(sub), null);

        subscriptionHandlers.put(topicFilter, new SubscriptionEntry(qos, handler));
        session.addSubscription(topicFilter, qos);

        try {
            sendPacket(packet);
            LOG.info(() -> "Subscribed to '" + topicFilter + "' QoS " + qos.value());
        } catch (Exception e) {
            session.packetIdAllocator().release(packetId);
            subscriptionHandlers.remove(topicFilter);
            session.removeSubscription(topicFilter);
            throw new MqttException("Subscribe failed", e);
        }
    }

    /**
     * Unsubscribes from a topic filter.
     */
    public void unsubscribe(String topicFilter) {
        ensureConnected();
        Objects.requireNonNull(topicFilter, "topicFilter");

        int packetId = session.packetIdAllocator().allocate();
        UnsubscribePacket packet = new UnsubscribePacket(packetId, List.of(topicFilter), null);

        try {
            sendPacket(packet);
            subscriptionHandlers.remove(topicFilter);
            session.removeSubscription(topicFilter);
            LOG.info(() -> "Unsubscribed from '" + topicFilter + "'");
        } catch (Exception e) {
            session.packetIdAllocator().release(packetId);
            throw new MqttException("Unsubscribe failed", e);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Packet Sending
    // ═══════════════════════════════════════════════════════════════════════

    private void sendPacket(MqttPacket packet) {
        writeLock.lock();
        try {
            byte[] encoded = MqttPacketEncoder.encode(packet, config.version());
            outputStream.write(encoded);
            outputStream.flush();

            if (keepAliveManager != null) {
                keepAliveManager.notifyPacketSent();
            }

            LOG.fine(() -> "Sent: " + packet.type());
        } catch (IOException e) {
            throw new MqttException.ConnectionLostException("Failed to send packet: " + packet.type(), e);
        } finally {
            writeLock.unlock();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Packet Reading (Read Loop)
    // ═══════════════════════════════════════════════════════════════════════

    private void startReadLoop() {
        readLoopThread = Thread.ofVirtual()
            .name("mqtt4j-read-" + config.clientId())
            .start(() -> {
                try {
                    InputStream in = transport.getInputStream();
                    while (connected.get() || connecting.get()) {
                        MqttPacket packet = MqttPacketDecoder.decode(in, config.version());
                        if (keepAliveManager != null) {
                            keepAliveManager.notifyPacketReceived();
                        }
                        handleIncomingPacket(packet);
                    }
                } catch (IOException e) {
                    if (connected.get()) {
                        LOG.log(Level.WARNING, "Connection lost: " + e.getMessage());
                        handleConnectionLost(e);
                    }
                } catch (Exception e) {
                    if (connected.get()) {
                        LOG.log(Level.SEVERE, "Read loop error", e);
                        handleConnectionLost(e);
                    }
                }
            });
    }

    private void handleIncomingPacket(MqttPacket packet) {
        LOG.fine(() -> "Received: " + packet.type());

        switch (packet) {
            case ConnAckPacket connAck -> handleConnAck(connAck);
            case PublishPacket publish -> handlePublish(publish);
            case PubAckPacket pubAck -> handlePubAck(pubAck);
            case PubRecPacket pubRec -> handlePubRec(pubRec);
            case PubRelPacket pubRel -> handlePubRel(pubRel);
            case PubCompPacket pubComp -> handlePubComp(pubComp);
            case SubAckPacket subAck -> handleSubAck(subAck);
            case UnsubAckPacket unsubAck -> handleUnsubAck(unsubAck);
            case PingRespPacket ignored -> handlePingResp();
            case DisconnectPacket disconnect -> handleDisconnect(disconnect);
            default -> LOG.warning(() -> "Unhandled packet type: " + packet.type());
        }
    }

    // ── CONNACK ────────────────────────────────────────────────────────────

    private void handleConnAck(ConnAckPacket connAck) {
        if (connAck.returnCode() != 0) {
            String reason = "Connection refused: code=" + connAck.returnCode();
            if (connAck.reasonCode() != null) {
                reason = "Connection refused: " + connAck.reasonCode().description();
            }
            connecting.set(false);
            connectFuture.completeExceptionally(
                new MqttException.ConnectionException(connAck.returnCode(), reason));
            closeTransport();
            return;
        }

        connected.set(true);
        connecting.set(false);

        LOG.info(() -> "Connected to " + config.host() + ":" + config.port() +
                       " [sessionPresent=" + connAck.sessionPresent() + "]");

        // Start keep-alive if configured
        if (config.keepAliveSeconds() > 0) {
            keepAliveManager = new KeepAliveManager(
                config.keepAliveSeconds(),
                () -> sendPacket(PingReqPacket.INSTANCE),
                () -> {
                    LOG.warning("Keep-alive timeout — connection lost");
                    handleConnectionLost(new MqttException.TimeoutException("Keep-alive timeout"));
                }
            );
            keepAliveManager.start();
        }

        // Setup auto-reconnect
        if (config.autoReconnect()) {
            setupReconnectManager();
        }

        // If session not present, re-subscribe
        if (!connAck.sessionPresent() && !subscriptionHandlers.isEmpty()) {
            resubscribeAll();
        }

        // Notify callbacks
        connectedCallbacks.forEach(cb -> messageExecutor.submit(cb::run));

        // Complete the connect future
        connectFuture.complete(null);

        // Reset reconnect manager on successful connect
        if (reconnectManager != null) {
            reconnectManager.reset();
        }
    }

    // ── PUBLISH (incoming) ─────────────────────────────────────────────────

    private void handlePublish(PublishPacket publish) {
        // Send acknowledgments based on QoS
        switch (publish.qos()) {
            case AT_LEAST_ONCE -> {
                // QoS 1: send PUBACK
                sendPacket(new PubAckPacket(publish.packetId(), null, null));
            }
            case EXACTLY_ONCE -> {
                // QoS 2: send PUBREC
                sendPacket(new PubRecPacket(publish.packetId(), null, null));
            }
            default -> {} // QoS 0: no acknowledgment
        }

        // Dispatch to matching handlers on virtual threads
        MqttMessage message = new MqttMessage(
            publish.topicName(), publish.payload(), publish.qos(), publish.retain());

        for (var entry : subscriptionHandlers.entrySet()) {
            if (MqttSession.matchTopic(entry.getKey(), publish.topicName())) {
                messageExecutor.submit(() -> {
                    try {
                        entry.getValue().handler().onMessage(publish.topicName(), message);
                    } catch (Exception e) {
                        LOG.log(Level.WARNING,
                            "Error in message handler for topic: " + publish.topicName(), e);
                    }
                });
            }
        }
    }

    // ── QoS 1/2 flow ──────────────────────────────────────────────────────

    private void handlePubAck(PubAckPacket pubAck) {
        session.inflightManager().handlePubAck(pubAck.packetId());
        session.packetIdAllocator().release(pubAck.packetId());
    }

    private void handlePubRec(PubRecPacket pubRec) {
        session.inflightManager().handlePubRec(pubRec.packetId());
        // Send PUBREL
        sendPacket(new PubRelPacket(pubRec.packetId(), null, null));
    }

    private void handlePubRel(PubRelPacket pubRel) {
        // Send PUBCOMP
        sendPacket(new PubCompPacket(pubRel.packetId(), null, null));
    }

    private void handlePubComp(PubCompPacket pubComp) {
        session.inflightManager().handlePubComp(pubComp.packetId());
        session.packetIdAllocator().release(pubComp.packetId());
    }

    // ── SUBACK / UNSUBACK ──────────────────────────────────────────────────

    private void handleSubAck(SubAckPacket subAck) {
        session.packetIdAllocator().release(subAck.packetId());
        LOG.fine(() -> "SUBACK received: packetId=" + subAck.packetId() +
                       ", codes=" + subAck.reasonCodes());
    }

    private void handleUnsubAck(UnsubAckPacket unsubAck) {
        session.packetIdAllocator().release(unsubAck.packetId());
        LOG.fine(() -> "UNSUBACK received: packetId=" + unsubAck.packetId());
    }

    // ── PINGRESP ───────────────────────────────────────────────────────────

    private void handlePingResp() {
        if (keepAliveManager != null) {
            keepAliveManager.notifyPongReceived();
        }
        LOG.finest("PINGRESP received");
    }

    // ── DISCONNECT (v5.0 server-initiated) ─────────────────────────────────

    private void handleDisconnect(DisconnectPacket disconnect) {
        String reason = "Server disconnected";
        if (disconnect.reasonCode() != null) {
            reason = "Server disconnected: " + disconnect.reasonCode().description();
        }
        LOG.warning(reason);
        handleConnectionLost(new MqttException.ConnectionLostException(reason));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Connection Lost / Reconnect
    // ═══════════════════════════════════════════════════════════════════════

    private void handleConnectionLost(Throwable cause) {
        if (!connected.compareAndSet(true, false)) {
            return; // already handling
        }

        // Stop keep-alive
        if (keepAliveManager != null) {
            keepAliveManager.stop();
        }

        // Complete any pending inflight messages exceptionally
        session.inflightManager().clear(
            new MqttException.ConnectionLostException("Connection lost", cause));

        // Close transport
        closeTransport();

        // Notify callbacks
        connectionLostCallbacks.forEach(cb ->
            messageExecutor.submit(() -> cb.accept(cause)));

        // Attempt reconnect
        if (config.autoReconnect() && !closed.get() && reconnectManager != null) {
            LOG.info("Attempting auto-reconnect...");
            reconnectManager.start();
        }
    }

    private void setupReconnectManager() {
        reconnectManager = new ReconnectManager(
            config.reconnectInitialDelayMs(),
            config.reconnectMaxDelayMs(),
            config.reconnectMultiplier(),
            config.reconnectMaxRetries(),
            () -> {
                try {
                    doConnect();
                    return true;
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Reconnect attempt failed", e);
                    return false;
                }
            }
        );
        reconnectManager.onReconnected(() ->
            LOG.info("Reconnected to " + config.host() + ":" + config.port()));
        reconnectManager.onReconnectFailed(e ->
            LOG.severe("Auto-reconnect failed permanently: " + e.getMessage()));
    }

    private void resubscribeAll() {
        LOG.info(() -> "Re-subscribing to " + subscriptionHandlers.size() + " topic(s)...");
        for (var entry : subscriptionHandlers.entrySet()) {
            int packetId = session.packetIdAllocator().allocate();
            SubscribePacket.Subscription sub = SubscribePacket.Subscription.of(
                entry.getKey(), entry.getValue().qos());
            SubscribePacket packet = new SubscribePacket(packetId, List.of(sub), null);
            try {
                sendPacket(packet);
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Failed to re-subscribe to: " + entry.getKey(), e);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Closes the client and releases all resources.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        if (connected.get()) {
            disconnect();
        }

        if (reconnectManager != null) {
            reconnectManager.stop();
        }

        messageExecutor.shutdown();
        try {
            if (!messageExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                messageExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            messageExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOG.info("MqttClient closed: " + config.clientId());
    }

    /** Returns the client configuration. */
    public MqttClientConfig config() {
        return config;
    }

    /** Returns the session state. */
    public MqttSession session() {
        return session;
    }

    // ── Internal helpers ───────────────────────────────────────────────────

    private void ensureConnected() {
        if (!connected.get()) {
            throw new MqttException("Client is not connected");
        }
    }

    private void closeTransport() {
        try {
            if (transport != null) {
                transport.close();
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "Error closing transport", e);
        }
        transport = null;
        outputStream = null;
    }
}
