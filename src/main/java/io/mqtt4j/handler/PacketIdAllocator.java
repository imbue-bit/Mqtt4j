package io.mqtt4j.handler;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe MQTT Packet Identifier allocator.
 *
 * <p>MQTT Packet Identifiers are 16-bit unsigned integers in the range 1–65535
 * (0 is reserved). This allocator hands out IDs sequentially, skipping any
 * that are currently in use, and wraps around when reaching 65535.</p>
 *
 * <p>Thread safety is achieved through an {@link AtomicInteger} for the counter
 * and a {@link ConcurrentHashMap.KeySetView} for the in-use set. No explicit
 * locking is required.</p>
 *
 * <p>Usage pattern:</p>
 * <pre>{@code
 * PacketIdAllocator allocator = new PacketIdAllocator();
 * int id = allocator.allocate();
 * try {
 *     // use the packet ID
 * } finally {
 *     allocator.release(id);
 * }
 * }</pre>
 */
public final class PacketIdAllocator {

    /** Maximum packet ID (16-bit unsigned integer). */
    private static final int MAX_PACKET_ID = 65535;

    /** Minimum packet ID (0 is reserved in MQTT). */
    private static final int MIN_PACKET_ID = 1;

    private final AtomicInteger counter = new AtomicInteger(0);
    private final Set<Integer> inUse = ConcurrentHashMap.newKeySet();

    /**
     * Allocates the next available packet ID.
     *
     * <p>Scans sequentially from the current counter position, skipping IDs
     * that are already in use. If all 65535 IDs are in use, throws an
     * {@link IllegalStateException}.</p>
     *
     * @return a packet ID in the range [1, 65535]
     * @throws IllegalStateException if no IDs are available
     */
    public int allocate() {
        int startValue = counter.get();
        for (int i = 0; i < MAX_PACKET_ID; i++) {
            int next = nextId();
            if (inUse.add(next)) {
                return next;
            }
        }
        throw new IllegalStateException(
                "All packet IDs are in use (65535 in-flight messages)");
    }

    /**
     * Releases a previously allocated packet ID, making it available for reuse.
     *
     * @param packetId the packet ID to release
     * @throws IllegalArgumentException if the packet ID is out of range
     */
    public void release(int packetId) {
        validateId(packetId);
        inUse.remove(packetId);
    }

    /**
     * Checks whether a packet ID is currently in use.
     *
     * @param packetId the packet ID to check
     * @return {@code true} if the ID is currently allocated
     * @throws IllegalArgumentException if the packet ID is out of range
     */
    public boolean isInUse(int packetId) {
        validateId(packetId);
        return inUse.contains(packetId);
    }

    /**
     * Returns the number of currently allocated packet IDs.
     *
     * @return the count of in-use IDs
     */
    public int inUseCount() {
        return inUse.size();
    }

    /**
     * Returns an unmodifiable view of the currently in-use packet IDs.
     *
     * @return unmodifiable set of in-use IDs
     */
    public Set<Integer> inUseIds() {
        return Collections.unmodifiableSet(inUse);
    }

    /**
     * Resets the allocator, releasing all packet IDs and resetting the counter.
     */
    public void reset() {
        inUse.clear();
        counter.set(0);
    }

    /**
     * Atomically advances the counter and returns the next ID in [1, 65535].
     */
    private int nextId() {
        return counter.updateAndGet(current -> {
            int next = current + 1;
            return (next > MAX_PACKET_ID) ? MIN_PACKET_ID : next;
        });
    }

    private static void validateId(int packetId) {
        if (packetId < MIN_PACKET_ID || packetId > MAX_PACKET_ID) {
            throw new IllegalArgumentException(
                    "Packet ID out of range [1, 65535]: " + packetId);
        }
    }

    @Override
    public String toString() {
        return "PacketIdAllocator[inUse=" + inUse.size() + "]";
    }
}
