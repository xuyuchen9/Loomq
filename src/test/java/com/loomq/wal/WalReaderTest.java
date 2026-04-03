package com.loomq.wal;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalReaderTest {

    @TempDir
    Path tempDir;

    private WalWriter writer;
    private WalReader reader;

    @BeforeEach
    void setUp() throws IOException {
        WalConfig config = createTestWalConfig(tempDir.toString());
        writer = new WalWriter(config);
    }

    @AfterEach
    void tearDown() {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void testReadAllRecords() throws IOException {
        // Write records
        for (int i = 0; i < 10; i++) {
            writer.append(
                    "t_" + i,
                    "biz_" + i,
                    EventType.CREATE,
                    System.currentTimeMillis() + i,
                    ("payload_" + i).getBytes()
            );
        }
        writer.flush();

        // Read records
        reader = new WalReader(writer.getAllSegments(), false);
        List<WalRecord> records = reader.readAll();

        assertEquals(10, records.size());

        for (int i = 0; i < 10; i++) {
            assertEquals("t_" + i, records.get(i).getTaskId());
            assertEquals("biz_" + i, records.get(i).getBizKey());
        }
    }

    @Test
    void testIterateRecords() throws IOException {
        writer.append("t_001", "biz_001", EventType.CREATE, System.currentTimeMillis(), "p1".getBytes());
        writer.append("t_002", "biz_002", EventType.CANCEL, System.currentTimeMillis(), "p2".getBytes());
        writer.append("t_003", "biz_003", EventType.ACK, System.currentTimeMillis(), "p3".getBytes());
        writer.flush();

        reader = new WalReader(writer.getAllSegments(), false);

        List<WalRecord> records = new ArrayList<>();
        for (WalRecord record : reader) {
            records.add(record);
        }

        assertEquals(3, records.size());
        assertEquals(EventType.CREATE, records.get(0).getEventType());
        assertEquals(EventType.CANCEL, records.get(1).getEventType());
        assertEquals(EventType.ACK, records.get(2).getEventType());
    }

    @Test
    void testEmptyWal() throws IOException {
        reader = new WalReader(writer.getAllSegments(), false);
        List<WalRecord> records = reader.readAll();
        assertTrue(records.isEmpty());
    }

    private static WalConfig createTestWalConfig(String dataDir) {
        return new WalConfig() {
            @Override
            public String dataDir() {
                return dataDir;
            }

            @Override
            public int segmentSizeMb() {
                return 1;
            }

            @Override
            public String flushStrategy() {
                return "batch";
            }

            @Override
            public long batchFlushIntervalMs() {
                return 100;
            }

            @Override
            public boolean syncOnWrite() {
                return false;
            }
        };
    }
}
