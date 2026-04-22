package io.github.hectorvent.floci.services.kinesis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class AwsEventStreamEncoder {

    private AwsEventStreamEncoder() {}

    /**
     * Encodes the mandatory initial-response frame.
     * Must be the first frame sent in every SubscribeToShard response.
     * Botocore's get_initial_response() will reject the stream if this is absent.
     */
    public static byte[] encodeInitialResponse() throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":message-type", "initial-response");
        return encodeFrame(headers, "{}".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Encodes a single AWS binary event stream message (e.g. SubscribeToShardEvent).
     */
    public static byte[] encodeEvent(String eventType, String contentType, byte[] payload) throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":message-type", "event");
        headers.put(":event-type", eventType);
        headers.put(":content-type", contentType);
        return encodeFrame(headers, payload);
    }

    /**
     * Wire format:
     *   [total_byte_length: 4B BE]
     *   [headers_byte_length: 4B BE]
     *   [prelude_crc: 4B CRC32 over first 8 bytes]
     *   [headers: variable]
     *   [payload: variable]
     *   [message_crc: 4B CRC32 over all preceding bytes]
     */
    private static byte[] encodeFrame(Map<String, String> headers, byte[] payload) throws IOException {
        byte[] headersBytes = encodeHeaders(headers);

        int totalLength = 4 + 4 + 4 + headersBytes.length + payload.length + 4;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(totalLength);
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(totalLength);
        dos.writeInt(headersBytes.length);

        // Prelude CRC (CRC32 of first 8 bytes)
        dos.writeInt((int) crc32(baos.toByteArray()));

        dos.write(headersBytes);
        dos.write(payload);
        dos.flush();

        // Message CRC (CRC32 of everything so far)
        dos.writeInt((int) crc32(baos.toByteArray()));
        dos.flush();

        return baos.toByteArray();
    }

    private static byte[] encodeHeaders(Map<String, String> headers) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            byte[] nameBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
            dos.writeByte(nameBytes.length);
            dos.write(nameBytes);
            dos.writeByte(7); // string value type
            dos.writeShort(valueBytes.length);
            dos.write(valueBytes);
        }
        dos.flush();
        return baos.toByteArray();
    }

    private static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }
}