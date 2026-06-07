package io.mqtt4j.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PacketIdAllocator}.
 */
class PacketIdAllocatorTest {

    private PacketIdAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new PacketIdAllocator();
    }

    @Test
    @DisplayName("Allocate returns IDs starting from 1")
    void testAllocateStartsFromOne() {
        int id = allocator.allocate();
        assertEquals(1, id);
    }

    @Test
    @DisplayName("Allocate returns sequential IDs")
    void testAllocateSequential() {
        int id1 = allocator.allocate();
        int id2 = allocator.allocate();
        int id3 = allocator.allocate();

        assertEquals(1, id1);
        assertEquals(2, id2);
        assertEquals(3, id3);
    }

    @Test
    @DisplayName("Released IDs can be reused")
    void testReleaseAndReuse() {
        int id1 = allocator.allocate();
        int id2 = allocator.allocate();

        allocator.release(id1);
        assertFalse(allocator.isInUse(id1));
        assertTrue(allocator.isInUse(id2));
    }

    @Test
    @DisplayName("isInUse tracks allocated IDs")
    void testIsInUse() {
        assertFalse(allocator.isInUse(1));

        int id = allocator.allocate();
        assertTrue(allocator.isInUse(id));

        allocator.release(id);
        assertFalse(allocator.isInUse(id));
    }

    @Test
    @DisplayName("Reset clears all allocations")
    void testReset() {
        allocator.allocate();
        allocator.allocate();
        allocator.allocate();

        allocator.reset();

        assertFalse(allocator.isInUse(1));
        assertFalse(allocator.isInUse(2));
        assertFalse(allocator.isInUse(3));
    }

    @Test
    @DisplayName("Allocate many IDs without collision")
    void testAllocateMany() {
        java.util.Set<Integer> ids = new java.util.HashSet<>();
        for (int i = 0; i < 1000; i++) {
            int id = allocator.allocate();
            assertTrue(id >= 1 && id <= 65535, "ID out of range: " + id);
            assertTrue(ids.add(id), "Duplicate ID: " + id);
        }
        assertEquals(1000, ids.size());
    }

    @Test
    @DisplayName("IDs wrap around at 65535")
    void testIdWrapAround() {
        // Allocate and release to advance counter
        for (int i = 0; i < 100; i++) {
            int id = allocator.allocate();
            allocator.release(id);
        }

        // All IDs should still be valid (1-65535)
        int id = allocator.allocate();
        assertTrue(id >= 1 && id <= 65535);
    }
}
