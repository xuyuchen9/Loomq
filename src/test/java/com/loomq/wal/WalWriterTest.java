package com.loomq.wal;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalWriterTest {

    @TempDir
    Path tempDir;

    private WalWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        WalConfig config = createTestWalConfig(tempDir.toString());
        writer = new WalWriter(config);
    }

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void testAppendSingleRecord() throws IOException {
        long pos = writer.append(
                "t_001",
                "biz_001",
                EventType.CREATE,
                System.currentTimeMillis(),
                "payload".getBytes()
        );

        assertTrue(pos >= 0);
        assertEquals(1, writer.getTotalRecords());
    }

    @Test
    void testAppendMultipleRecords() throws IOException {
        for (int i = 0; i < 100; i++) {
            writer.append(
                    "t_" + i,
                    "biz_" + i,
                    EventType.CREATE,
                    System.currentTimeMillis() + i,
                    ("payload_" + i).getBytes()
            );
        }

        assertEquals(100, writer.getTotalRecords());
    }

    @Test
    void testFlush() throws IOException {
        writer.append(
                "t_001",
                "biz_001",
                EventType.CREATE,
                System.currentTimeMillis(),
                "payload".getBytes()
        );

        assertDoesNotThrow(() -> writer.flush());
    }

    @Test
    void testGetAllSegments() throws IOException {
        writer.append(
                "t_001",
                "biz_001",
                EventType.CREATE,
                System.currentTimeMillis(),
                "payload".getBytes()
        );

        List<WalSegment> segments = writer.getAllSegments();
        assertNotNull(segments);
        assertEquals(1, segments.size());
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
