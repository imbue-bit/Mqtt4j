package io.mqtt4j.codec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link MqttCodecUtil} — Variable Byte Integer, UTF-8 String,
 * Binary Data, and fixed-width integer encoding/decoding.
 */
class MqttCodecUtilTest {

    // ── Variable Byte Integer ──────────────────────────────────────────────

    @ParameterizedTest(name = "VBI encode/decode: {0}")
    @ValueSource(ints = {0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455})
    @DisplayName("Variable Byte Integer round-trip")
    void testVariableByteIntegerRoundTrip(int value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeVariableByteInteger(out, value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int decoded = MqttCodecUtil.decodeVariableByteInteger(in);

        assertEquals(value, decoded, "VBI round-trip failed for " + value);
    }

    @Test
    @DisplayName("VBI encoding byte counts")
    void testVariableByteIntegerSizes() throws IOException {
        assertEquals(1, encodeAndGetSize(0));
        assertEquals(1, encodeAndGetSize(127));
        assertEquals(2, encodeAndGetSize(128));
        assertEquals(2, encodeAndGetSize(16383));
        assertEquals(3, encodeAndGetSize(16384));
        assertEquals(3, encodeAndGetSize(2097151));
        assertEquals(4, encodeAndGetSize(2097152));
        assertEquals(4, encodeAndGetSize(268435455));
    }

    @Test
    @DisplayName("VBI size calculation")
    void testVariableByteIntegerSizeCalculation() {
        assertEquals(1, MqttCodecUtil.variableByteIntegerSize(0));
        assertEquals(1, MqttCodecUtil.variableByteIntegerSize(127));
        assertEquals(2, MqttCodecUtil.variableByteIntegerSize(128));
        assertEquals(2, MqttCodecUtil.variableByteIntegerSize(16383));
        assertEquals(3, MqttCodecUtil.variableByteIntegerSize(16384));
        assertEquals(4, MqttCodecUtil.variableByteIntegerSize(268435455));
    }

    @Test
    @DisplayName("VBI rejects values out of range")
    void testVariableByteIntegerOutOfRange() {
        assertThrows(Exception.class, () -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MqttCodecUtil.encodeVariableByteInteger(out, 268435456);
        });
        assertThrows(Exception.class, () -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MqttCodecUtil.encodeVariableByteInteger(out, -1);
        });
    }

    @Test
    @DisplayName("VBI specific byte patterns")
    void testVariableByteIntegerKnownPatterns() throws IOException {
        // 0 → [0x00]
        assertArrayEquals(new byte[]{0x00}, MqttCodecUtil.encodeVariableByteInteger(0));

        // 127 → [0x7F]
        assertArrayEquals(new byte[]{0x7F}, MqttCodecUtil.encodeVariableByteInteger(127));

        // 128 → [0x80, 0x01]
        assertArrayEquals(new byte[]{(byte) 0x80, 0x01}, MqttCodecUtil.encodeVariableByteInteger(128));

        // 16383 → [0xFF, 0x7F]
        assertArrayEquals(new byte[]{(byte) 0xFF, 0x7F}, MqttCodecUtil.encodeVariableByteInteger(16383));
    }

    // ── UTF-8 String ───────────────────────────────────────────────────────

    @ParameterizedTest(name = "UTF-8 round-trip: \"{0}\"")
    @ValueSource(strings = {"", "hello", "MQTT", "sensor/温度/data", "🌡️", "a/b/c/d/e/f"})
    @DisplayName("UTF-8 String round-trip")
    void testUtf8StringRoundTrip(String value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeUtf8String(out, value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        String decoded = MqttCodecUtil.decodeUtf8String(in);

        assertEquals(value, decoded);
    }

    @Test
    @DisplayName("UTF-8 String encoding format")
    void testUtf8StringFormat() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeUtf8String(out, "MQTT");
        byte[] encoded = out.toByteArray();

        // 2-byte length prefix + 4 bytes "MQTT"
        assertEquals(6, encoded.length);
        assertEquals(0x00, encoded[0]); // length MSB
        assertEquals(0x04, encoded[1]); // length LSB
        assertEquals('M', encoded[2]);
        assertEquals('Q', encoded[3]);
        assertEquals('T', encoded[4]);
        assertEquals('T', encoded[5]);
    }

    @Test
    @DisplayName("UTF-8 String size calculation")
    void testUtf8StringSize() {
        assertEquals(2, MqttCodecUtil.utf8StringSize("")); // 2 bytes length + 0 content
        assertEquals(6, MqttCodecUtil.utf8StringSize("MQTT")); // 2 + 4
        assertEquals(5, MqttCodecUtil.utf8StringSize("abc")); // 2 + 3
    }

    // ── Binary Data ────────────────────────────────────────────────────────

    @Test
    @DisplayName("Binary Data round-trip")
    void testBinaryDataRoundTrip() throws IOException {
        byte[] data = {0x01, 0x02, (byte) 0xFF, 0x00, (byte) 0xAB};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeBinaryData(out, data);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] decoded = MqttCodecUtil.decodeBinaryData(in);

        assertArrayEquals(data, decoded);
    }

    @Test
    @DisplayName("Empty binary data round-trip")
    void testEmptyBinaryData() throws IOException {
        byte[] data = new byte[0];

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeBinaryData(out, data);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] decoded = MqttCodecUtil.decodeBinaryData(in);

        assertArrayEquals(data, decoded);
        assertEquals(2, out.size()); // just the 2-byte length prefix
    }

    // ── Unsigned Short (2-byte) ────────────────────────────────────────────

    @ParameterizedTest(name = "Unsigned short: {0}")
    @ValueSource(ints = {0, 1, 255, 256, 1883, 8883, 65535})
    @DisplayName("Unsigned short round-trip")
    void testUnsignedShortRoundTrip(int value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeUnsignedShort(out, value);

        assertEquals(2, out.size());

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int decoded = MqttCodecUtil.decodeUnsignedShort(in);

        assertEquals(value, decoded);
    }

    // ── Unsigned Int (4-byte) ──────────────────────────────────────────────

    @ParameterizedTest(name = "Unsigned int: {0}")
    @CsvSource({"0", "1", "65535", "65536", "3600", "4294967295"})
    @DisplayName("Unsigned int round-trip")
    void testUnsignedIntRoundTrip(long value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeUnsignedInt(out, value);

        assertEquals(4, out.size());

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        long decoded = MqttCodecUtil.decodeUnsignedInt(in);

        assertEquals(value, decoded);
    }

    // ── readBytes ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("readBytes reads exact count")
    void testReadBytesExact() throws IOException {
        byte[] data = {1, 2, 3, 4, 5};
        ByteArrayInputStream in = new ByteArrayInputStream(data);

        byte[] result = MqttCodecUtil.readBytes(in, 3);
        assertArrayEquals(new byte[]{1, 2, 3}, result);

        result = MqttCodecUtil.readBytes(in, 2);
        assertArrayEquals(new byte[]{4, 5}, result);
    }

    @Test
    @DisplayName("readBytes with zero count")
    void testReadBytesZero() throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{1, 2});
        byte[] result = MqttCodecUtil.readBytes(in, 0);
        assertArrayEquals(new byte[0], result);
    }

    @Test
    @DisplayName("readBytes throws on EOF")
    void testReadBytesEof() {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{1, 2});
        assertThrows(IOException.class, () -> MqttCodecUtil.readBytes(in, 5));
    }

    @Test
    @DisplayName("readByte throws on EOF")
    void testReadByteEof() {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        assertThrows(IOException.class, () -> MqttCodecUtil.readByte(in));
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private int encodeAndGetSize(int value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MqttCodecUtil.encodeVariableByteInteger(out, value);
        return out.size();
    }
}
