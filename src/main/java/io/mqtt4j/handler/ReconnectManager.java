package io.mqtt4j.handler;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Automatic reconnection manager with exponential backoff and jitter.
 *
 * <p>When started, launches a virtual thread that repeatedly attempts to
 * reconnect using the provided {@code reconnectAction}. The delay between
 * attempts grows exponentially from {@code initialDelayMs} up to
 * {@code maxDelayMs}, with random jitter to avoid thundering herd.</p>
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li><b>initialDelayMs</b> — first retry delay (default: 1000ms)</li>
 *   <li><b>maxDelayMs</b> — cap on retry delay (default: 30000ms)</li>
 *   <li><b>multiplier</b> — backoff multiplier (default: 2.0)</li>
 *   <li><b>maxRetries</b> — max attempts, -1 for infinite (default: -1)</li>
 * </ul>
 *
 * <h3>Jitter:</h3>
 * <p>Each delay is jittered by ±25% to spread out reconnection attempts
 * from multiple clients.</p>
 *
 * <h3>Callbacks:</h3>
 * <ul>
 *   <li>{@link #onReconnected(Runnable)} — called when reconnection succeeds</li>
 *   <li>{@link #onReconnectFailed(Consumer)} — called when a reconnection attempt fails</li>
 * </ul>
 *
 * @see #start()
 * @see #stop()
 */
public final class ReconnectManager {

    private static final Logger LOG = Logger.getLogger(ReconnectManager.class.getName());

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final int maxRetries;
    private final Callable<Boolean> reconnectAction;

    private volatile Runnable onReconnectedCallback;
    private volatile Consumer<Exception> onReconnectFailedCallback;

    private volatile Thread reconnectThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile long currentDelayMs;

    /**
     * Creates a reconnect manager with full configuration.
     *
     * @param initialDelayMs  initial delay between retries in milliseconds
     * @param maxDelayMs      maximum delay cap in milliseconds
     * @param multiplier      exponential backoff multiplier (e.g. 2.0)
     * @param maxRetries      maximum number of retry attempts (-1 for infinite)
     * @param reconnectAction action to attempt reconnection; returns {@code true} on success
     * @throws NullPointerException     if reconnectAction is null
     * @throws IllegalArgumentException if delays or multiplier are invalid
     */
    public ReconnectManager(long initialDelayMs, long maxDelayMs, double multiplier,
                            int maxRetries, Callable<Boolean> reconnectAction) {
        if (initialDelayMs < 1) {
            throw new IllegalArgumentException("initialDelayMs must be >= 1");
        }
        if (maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException("maxDelayMs must be >= initialDelayMs");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
        this.maxRetries = maxRetries;
        this.reconnectAction = Objects.requireNonNull(reconnectAction, "reconnectAction");
        this.currentDelayMs = initialDelayMs;
    }

    /**
     * Creates a reconnect manager with default settings.
     *
     * <p>Defaults: initialDelay=1000ms, maxDelay=30000ms, multiplier=2.0, infinite retries.</p>
     *
     * @param reconnectAction action to attempt reconnection; returns {@code true} on success
     */
    public ReconnectManager(Callable<Boolean> reconnectAction) {
        this(1000, 30000, 2.0, -1, reconnectAction);
    }

    /**
     * Registers a callback invoked when reconnection succeeds.
     *
     * @param callback the success callback
     */
    public void onReconnected(Runnable callback) {
        this.onReconnectedCallback = callback;
    }

    /**
     * Registers a callback invoked when a reconnection attempt fails.
     *
     * <p>This is called for each individual failure, not just the final one.</p>
     *
     * @param callback the failure callback, receiving the exception
     */
    public void onReconnectFailed(Consumer<Exception> callback) {
        this.onReconnectFailedCallback = callback;
    }

    /**
     * Starts the reconnection loop on a virtual thread.
     *
     * <p>If already running, this method does nothing.</p>
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.log(Level.FINE, "Reconnect manager already running");
            return;
        }
        currentDelayMs = initialDelayMs;

        reconnectThread = Thread.ofVirtual()
                .name("mqtt-reconnect")
                .start(this::reconnectLoop);

        LOG.log(Level.INFO, "Reconnect manager started (initialDelay={0}ms, maxDelay={1}ms, " +
                        "multiplier={2}, maxRetries={3})",
                new Object[]{initialDelayMs, maxDelayMs, multiplier, maxRetries});
    }

    /**
     * Stops the reconnection loop.
     *
     * <p>Interrupts the reconnection thread and resets the running flag.</p>
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Thread t = reconnectThread;
        reconnectThread = null;
        if (t != null) {
            t.interrupt();
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.log(Level.INFO, "Reconnect manager stopped");
    }

    /**
     * Resets the backoff delay back to the initial value.
     *
     * <p>Useful after a successful connection to prepare for a future disconnection.</p>
     */
    public void reset() {
        currentDelayMs = initialDelayMs;
    }

    /**
     * Returns whether the reconnect manager is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    // ── Reconnection loop ─────────────────────────────────────────────────

    private void reconnectLoop() {
        int attempt = 0;

        while (running.get()) {
            attempt++;

            if (maxRetries >= 0 && attempt > maxRetries) {
                LOG.log(Level.WARNING, "Max reconnect retries ({0}) exhausted", maxRetries);
                running.set(false);
                return;
            }

            // Wait with jittered backoff
            long jitteredDelay = applyJitter(currentDelayMs);
            LOG.log(Level.FINE, "Reconnect attempt {0} in {1}ms (base delay={2}ms)",
                    new Object[]{attempt, jitteredDelay, currentDelayMs});

            try {
                Thread.sleep(jitteredDelay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.log(Level.FINE, "Reconnect sleep interrupted");
                return;
            }

            if (!running.get()) break;

            // Attempt reconnection
            try {
                boolean success = reconnectAction.call();

                if (success) {
                    LOG.log(Level.INFO, "Reconnected successfully on attempt {0}", attempt);
                    currentDelayMs = initialDelayMs; // reset backoff
                    running.set(false);

                    Runnable successCb = onReconnectedCallback;
                    if (successCb != null) {
                        try {
                            successCb.run();
                        } catch (Exception e) {
                            LOG.log(Level.WARNING, "onReconnected callback error", e);
                        }
                    }
                    return;
                } else {
                    LOG.log(Level.FINE, "Reconnect attempt {0} returned false", attempt);
                    escalateDelay();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Reconnect attempt {0} failed: {1}",
                        new Object[]{attempt, e.getMessage()});

                Consumer<Exception> failCb = onReconnectFailedCallback;
                if (failCb != null) {
                    try {
                        failCb.accept(e);
                    } catch (Exception cbError) {
                        LOG.log(Level.WARNING, "onReconnectFailed callback error", cbError);
                    }
                }
                escalateDelay();
            }
        }
    }

    /**
     * Increases the delay by the multiplier, capped at maxDelayMs.
     */
    private void escalateDelay() {
        currentDelayMs = Math.min((long) (currentDelayMs * multiplier), maxDelayMs);
    }

    /**
     * Applies ±25% random jitter to the given delay.
     */
    private static long applyJitter(long delayMs) {
        double jitterFactor = 0.75 + (ThreadLocalRandom.current().nextDouble() * 0.5); // 0.75–1.25
        return Math.max(1, (long) (delayMs * jitterFactor));
    }

    @Override
    public String toString() {
        return "ReconnectManager[running=" + running.get() +
                ", delay=" + currentDelayMs + "ms" +
                ", maxRetries=" + maxRetries + "]";
    }
}
