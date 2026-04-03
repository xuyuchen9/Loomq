package com.loomq.wal;

import com.loomq.entity.EventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalRecordTest {

    @Test
    void testEncodeAndDecode() {
        WalRecord original = WalRecord.create(
                0, 1, "t_1234567890_000001_000",
                "biz_key_001",
                EventType.CREATE,
                System.currentTimeMillis(),
                "{\"test\":\"data\"}".getBytes()
        );

        byte[] encoded = original.encode();
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        WalRecord decoded = WalRecord.decode(encoded);
        assertEquals(original.getTaskId(), decoded.getTaskId());
        assertEquals(original.getBizKey(), decoded.getBizKey());
        assertEquals(original.getEventType(), decoded.getEventType());
        assertEquals(original.getEventTime(), decoded.getEventTime());
        assertEquals(original.getSegmentSeq(), decoded.getSegmentSeq());
        assertEquals(original.getRecordSeq(), decoded.getRecordSeq());
    }

    @Test
    void testEncodeWithNullBizKey() {
        WalRecord original = WalRecord.create(
                0, 1, "t_1234567890_000001_000",
                null,
                EventType.CREATE,
                System.currentTimeMillis(),
                new byte[0]
        );

        byte[] encoded = original.encode();
        WalRecord decoded = WalRecord.decode(encoded);

        assertEquals(original.getTaskId(), decoded.getTaskId());
        assertNull(decoded.getBizKey());
    }

    @Test
    void testChecksumValidation() {
        WalRecord original = WalRecord.create(
                0, 1, "t_1234567890_000001_000",
                "biz_key",
                EventType.CREATE,
                System.currentTimeMillis(),
                "payload".getBytes()
        );

        byte[] encoded = original.encode();

        // Corrupt the data
        encoded[50] = (byte) (encoded[50] ^ 0xFF);

        assertThrows(IllegalArgumentException.class, () -> WalRecord.decode(encoded));
    }

    @Test
    void testInvalidMagic() {
        WalRecord original = WalRecord.create(
                0, 1, "t_1234567890_000001_000",
                "biz_key",
                EventType.CREATE,
                System.currentTimeMillis(),
                "payload".getBytes()
        );

        byte[] encoded = original.encode();
        encoded[0] = 0; // Corrupt magic

        assertThrows(IllegalArgumentException.class, () -> WalRecord.decode(encoded));
    }

    @Test
    void testAllEventTypes() {
        for (EventType type : EventType.values()) {
            WalRecord original = WalRecord.create(
                    0, 1, "t_test_" + type.getCode(),
                    "biz_key",
                    type,
                    System.currentTimeMillis(),
                    "payload".getBytes()
            );

            byte[] encoded = original.encode();
            WalRecord decoded = WalRecord.decode(encoded);

            assertEquals(type, decoded.getEventType());
        }
    }
}
