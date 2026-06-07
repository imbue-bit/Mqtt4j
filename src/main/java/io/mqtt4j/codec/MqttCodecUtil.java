package io.mqtt4j.codec;

import io.mqtt4j.MqttException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Low-level codec utilities for MQTT wire format encoding and decoding.
 *
 * <p>Covers Variable Byte Integer, UTF-8 String, Binary Data, and fixed-width
 * integer encoding as specified in the MQTT v3.1.1 and v5.0 specifications.</p>
 */
public final class MqttCodecUtil {

    /** Maximum value encodable in a Variable Byte Integer (268,435,455 = 256 MB). */
    public static final int MAX_VARIABLE_BYTE_INTEGER = 268_435_455;

    private MqttCodecUtil() {} // utility class

    // ── Variable Byte Integer ──────────────────────────────────────────────

    /**
     * Encodes a Variable Byte Integer (1–4 bytes, continuation-bit encoding).
     *
     * @param out   the output stream
     * @param value the value to encode (0 to 268,435,455)
     * @return the number of bytes written (1–4)
     */
    public static int encodeVariableByteInteger(OutputStream out, int value) throws IOException {
        if (value < 0 || value > MAX_VARIABLE_BYTE_INTEGER) {
            throw new MqttException.CodecException(
                "Variable Byte Integer out of range: " + value);
        }
        int count = 0;
        do {
            int encodedByte = value & 0x7F;
            value >>= 7;
            if (value > 0) {
                encodedByte |= 0x80; // set continuation bit
            }
            out.write(encodedByte);
            count++;
        } while (value > 0);
        return count;
    }

    /**
     * Encodes a Variable Byte Integer and returns the bytes.
     */
    public static byte[] encodeVariableByteInteger(int value) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(4);
            encodeVariableByteInteger(baos, value);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new MqttException.CodecException("Failed to encode VBI", e);
        }
    }

    /**
     * Decodes a Variable Byte Integer from the input stream.
     *
     * @param in the input stream
     * @return the decoded value (0 to 268,435,455)
     */
    public static int decodeVariableByteInteger(InputStream in) throws IOException {
        int value = 0;
        int multiplier = 1;
        int encodedByte;
        int count = 0;

        do {
            encodedByte = readByte(in);
            value += (encodedByte & 0x7F) * multiplier;
            if (multiplier > 128 * 128 * 128) {
                throw new MqttException.CodecException(
                    "Variable Byte Integer exceeds maximum length (4 bytes)");
            }
            multiplier *= 128;
            count++;
        } while ((encodedByte & 0x80) != 0);

        return value;
    }

    // ── UTF-8 Encoded String ───────────────────────────────────────────────

    /**
     * Encodes a UTF-8 string with a 2-byte big-endian length prefix.
     */
    public static void encodeUtf8String(OutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 65535) {
            throw new MqttException.CodecException(
                "UTF-8 string too long: " + bytes.length + " bytes (max 65535)");
        }
        encodeUnsignedShort(out, bytes.length);
        out.write(bytes);
    }

    /**
     * Decodes a UTF-8 string with a 2-byte big-endian length prefix.
     */
    public static String decodeUtf8String(InputStream in) throws IOException {
        int length = decodeUnsignedShort(in);
        byte[] bytes = readBytes(in, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ── Binary Data ────────────────────────────────────────────────────────

    /**
     * Encodes binary data with a 2-byte big-endian length prefix.
     */
    public static void encodeBinaryData(OutputStream out, byte[] data) throws IOException {
        if (data.length > 65535) {
            throw new MqttException.CodecException(
                "Binary data too long: " + data.length + " bytes (max 65535)");
        }
        encodeUnsignedShort(out, data.length);
        out.write(data);
    }

    /**
     * Decodes binary data with a 2-byte big-endian length prefix.
     */
    public static byte[] decodeBinaryData(InputStream in) throws IOException {
        int length = decodeUnsignedShort(in);
        return readBytes(in, length);
    }

    // ── Fixed-width Integers ───────────────────────────────────────────────

    /**
     * Encodes a 2-byte unsigned integer (big-endian).
     */
    public static void encodeUnsignedShort(OutputStream out, int value) throws IOException {
        out.write((value >> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    /**
     * Decodes a 2-byte unsigned integer (big-endian).
     */
    public static int decodeUnsignedShort(InputStream in) throws IOException {
        int msb = readByte(in);
        int lsb = readByte(in);
        return (msb << 8) | lsb;
    }

    /**
     * Encodes a 4-byte unsigned integer (big-endian).
     */
    public static void encodeUnsignedInt(OutputStream out, long value) throws IOException {
        out.write((int) ((value >> 24) & 0xFF));
        out.write((int) ((value >> 16) & 0xFF));
        out.write((int) ((value >> 8) & 0xFF));
        out.write((int) (value & 0xFF));
    }

    /**
     * Decodes a 4-byte unsigned integer (big-endian).
     */
    public static long decodeUnsignedInt(InputStream in) throws IOException {
        long b1 = readByte(in);
        long b2 = readByte(in);
        long b3 = readByte(in);
        long b4 = readByte(in);
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    // ── Raw I/O Helpers ────────────────────────────────────────────────────

    /**
     * Reads a single byte from the input stream.
     *
     * @throws IOException if end of stream is reached
     */
    public static int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new IOException("Unexpected end of MQTT stream");
        }
        return b;
    }

    /**
     * Reads exactly {@code count} bytes from the input stream.
     *
     * @throws IOException if fewer bytes are available
     */
    public static byte[] readBytes(InputStream in, int count) throws IOException {
        if (count == 0) return new byte[0];
        byte[] buf = new byte[count];
        int offset = 0;
        while (offset < count) {
            int read = in.read(buf, offset, count - offset);
            if (read == -1) {
                throw new IOException(
                    "Unexpected end of MQTT stream: expected " + count +
                    " bytes, got " + offset);
            }
            offset += read;
        }
        return buf;
    }

    /**
     * Calculates the number of bytes needed to encode a Variable Byte Integer.
     */
    public static int variableByteIntegerSize(int value) {
        if (value < 0) throw new IllegalArgumentException("Negative value: " + value);
        if (value <= 127) return 1;
        if (value <= 16_383) return 2;
        if (value <= 2_097_151) return 3;
        if (value <= MAX_VARIABLE_BYTE_INTEGER) return 4;
        throw new MqttException.CodecException("Value exceeds VBI max: " + value);
    }

    /**
     * Calculates the encoded size of a UTF-8 string (2-byte prefix + UTF-8 bytes).
     */
    public static int utf8StringSize(String str) {
        return 2 + str.getBytes(StandardCharsets.UTF_8).length;
    }
}
