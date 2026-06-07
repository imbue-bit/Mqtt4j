package io.mqtt4j.handler;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT Keep-Alive manager.
 *
 * <p>Runs a virtual thread that monitors connection liveness according to the
 * MQTT keep-alive contract. If no MQTT packet has been sent within 75% of the
 * keep-alive interval, a PINGREQ is triggered. If no PINGRESP is received
 * within a reasonable timeout, the connection is considered dead.</p>
 *
 * <h3>Timing model:</h3>
 * <ul>
 *   <li><b>Ping interval:</b> {@code keepAliveSeconds × 0.75} — sends PINGREQ before
 *       the server's 1.5× deadline</li>
 *   <li><b>Pong timeout:</b> {@code keepAliveSeconds × 0.25} — time to wait for
 *       PINGRESP after sending PINGREQ</li>
 * </ul>
 *
 * <p>The caller provides two callbacks:</p>
 * <ul>
 *   <li>{@code pingCallback} — invoked when a PINGREQ should be sent</li>
 *   <li>{@code timeoutCallback} — invoked when the connection is considered dead</li>
 * </ul>
 *
 * <p>Uses {@code volatile} fields for timing state and {@link Thread#ofVirtual()}
 * for the keep-alive thread — no locking needed.</p>
 */
public final class KeepAliveManager {

    private static final Logger LOG = Logger.getLogger(KeepAliveManager.class.getName());

    private final int keepAliveSeconds;
    private final Runnable pingCallback;
    private final Runnable timeoutCallback;

    /** Time of last packet sent (any type), epoch millis. */
    private volatile long lastPacketSentMs;

    /** Time of last packet received (any type), epoch millis. */
    private volatile long lastPacketReceivedMs;

    /** Whether a PINGRESP has been received since the last PINGREQ. */
    private volatile boolean pongReceived;

    /** Whether a PINGREQ is currently outstanding. */
    private volatile boolean pingOutstanding;

    /** The keep-alive virtual thread. */
    private volatile Thread keepAliveThread;

    /** Whether the manager is running. */
    private volatile boolean running;

    /**
     * Creates a keep-alive manager.
     *
     * @param keepAliveSeconds the keep-alive interval in seconds (as negotiated with the broker)
     * @param pingCallback     invoked on the keep-alive thread when a PINGREQ should be sent
     * @param timeoutCallback  invoked on the keep-alive thread when the connection is considered dead
     * @throws IllegalArgumentException if keepAliveSeconds is &lt; 1
     * @throws NullPointerException     if any callback is null
     */
    public KeepAliveManager(int keepAliveSeconds, Runnable pingCallback, Runnable timeoutCallback) {
        if (keepAliveSeconds < 1) {
            throw new IllegalArgumentException("keepAliveSeconds must be >= 1, got: " + keepAliveSeconds);
        }
        this.keepAliveSeconds = keepAliveSeconds;
        this.pingCallback = Objects.requireNonNull(pingCallback, "pingCallback");
        this.timeoutCallback = Objects.requireNonNull(timeoutCallback, "timeoutCallback");
    }

    /**
     * Starts the keep-alive monitor on a virtual thread.
     *
     * <p>If already running, this method does nothing.</p>
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        long now = System.currentTimeMillis();
        lastPacketSentMs = now;
        lastPacketReceivedMs = now;
        pongReceived = false;
        pingOutstanding = false;

        keepAliveThread = Thread.ofVirtual()
                .name("mqtt-keepalive")
                .start(this::keepAliveLoop);

        LOG.log(Level.FINE, "Keep-alive started: interval={0}s", keepAliveSeconds);
    }

    /**
     * Stops the keep-alive monitor.
     *
     * <p>Interrupts the keep-alive thread and waits briefly for it to finish.</p>
     */
    public void stop() {
        running = false;
        Thread t = keepAliveThread;
        keepAliveThread = null;
        if (t != null) {
            t.interrupt();
            try {
                t.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.log(Level.FINE, "Keep-alive stopped");
    }

    /**
     * Notifies the manager that a packet (any type) has been sent.
     *
     * <p>Resets the send timer so PINGREQ is deferred.</p>
     */
    public void notifyPacketSent() {
        lastPacketSentMs = System.currentTimeMillis();
    }

    /**
     * Notifies the manager that a packet (any type) has been received.
     *
     * <p>Resets the receive timer.</p>
     */
    public void notifyPacketReceived() {
        lastPacketReceivedMs = System.currentTimeMillis();
    }

    /**
     * Notifies the manager that a PINGRESP has been received.
     *
     * <p>Clears the outstanding ping flag.</p>
     */
    public void notifyPongReceived() {
        pongReceived = true;
        pingOutstanding = false;
        lastPacketReceivedMs = System.currentTimeMillis();
    }

    /**
     * Returns whether the keep-alive manager is currently running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the configured keep-alive interval in seconds.
     */
    public int keepAliveSeconds() {
        return keepAliveSeconds;
    }

    // ── Keep-alive loop ───────────────────────────────────────────────────

    private void keepAliveLoop() {
        long pingIntervalMs = (long) (keepAliveSeconds * 750); // 75% of keep-alive
        long pongTimeoutMs = (long) (keepAliveSeconds * 250);  // 25% of keep-alive

        while (running) {
            try {
                // Sleep for the ping check interval
                Thread.sleep(pingIntervalMs);

                if (!running) break;

                long now = System.currentTimeMillis();
                long elapsedSinceSend = now - lastPacketSentMs;

                if (pingOutstanding) {
                    // A PINGREQ is outstanding — check if we've timed out
                    if (!pongReceived) {
                        long elapsedSinceReceive = now - lastPacketReceivedMs;
                        if (elapsedSinceReceive >= pingIntervalMs + pongTimeoutMs) {
                            LOG.log(Level.WARNING,
                                    "Keep-alive timeout: no PINGRESP received within {0}ms",
                                    pongTimeoutMs);
                            if (running) {
                                timeoutCallback.run();
                            }
                            return; // exit the loop — connection is dead
                        }
                    }
                    // Pong was received or still within timeout — continue
                    pingOutstanding = false;
                    pongReceived = false;
                    continue;
                }

                // No ping outstanding — check if we need to send one
                if (elapsedSinceSend >= pingIntervalMs) {
                    LOG.log(Level.FINE, "Sending PINGREQ (idle for {0}ms)", elapsedSinceSend);
                    pongReceived = false;
                    pingOutstanding = true;
                    try {
                        pingCallback.run();
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "PINGREQ callback failed", e);
                        if (running) {
                            timeoutCallback.run();
                        }
                        return;
                    }

                    // Wait for pong
                    Thread.sleep(pongTimeoutMs);

                    if (!running) break;

                    if (!pongReceived) {
                        LOG.log(Level.WARNING,
                                "Keep-alive timeout: no PINGRESP within {0}ms after PINGREQ",
                                pongTimeoutMs);
                        if (running) {
                            timeoutCallback.run();
                        }
                        return;
                    }

                    // Pong received — reset
                    pingOutstanding = false;
                    pongReceived = false;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.log(Level.FINE, "Keep-alive thread interrupted");
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "KeepAliveManager[interval=" + keepAliveSeconds + "s, running=" + running + "]";
    }
}
