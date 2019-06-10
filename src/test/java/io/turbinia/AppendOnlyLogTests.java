/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.turbinia;

import jdk.nio.mapmode.ExtendedMapMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import sun.misc.Unsafe;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ConcurrentModificationException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

@WithBytemanFrom(source = ExecutionTracer.class)
class AppendOnlyLogTests {

    static Unsafe unsafe;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static File file = new File("/mnt/pmem/tx/test");

    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    @BeforeEach
    public void setUp() throws IOException {

        if (file.exists()) {
            file.delete();
        }

        fileChannel = (FileChannel) Files
                .newByteChannel(file.toPath(), EnumSet.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE));

        mappedByteBuffer = fileChannel.map(ExtendedMapMode.READ_WRITE_SYNC, 0, 1024);
        forceInvalidation();
    }

    @AfterEach
    public void tearDown() throws IOException {

        // https://bugs.openjdk.java.net/browse/JDK-4724038
        unsafe.invokeCleaner(mappedByteBuffer);

        fileChannel.close();
    }

    private void forceInvalidation() {
        mappedByteBuffer.put(0, (byte) 0); // destroy the header so the log structure won't be recognized/recovered
    }

    @Test
    public void testIteratorExhaustion() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        appendOnlyLog.put(new byte[10]);

        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    public void testIteratorEpoch() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        appendOnlyLog.put(new byte[10]);

        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        assertTrue(iter.hasNext());
        appendOnlyLog.clear();
        assertThrows(ConcurrentModificationException.class, iter::hasNext);
        assertThrows(ConcurrentModificationException.class, iter::next);
    }

    @Test
    public void testIteratorCopying() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        byte[] data = new byte[]{(byte) 1};
        appendOnlyLog.put(data);

        Iterator<ByteBuffer> viewIterator = appendOnlyLog.iterator();
        assertTrue(viewIterator.hasNext());
        ByteBuffer view = viewIterator.next();
        assertEquals((byte) 1, view.get(0));

        Iterator<ByteBuffer> copyingIterator = appendOnlyLog.copyingIterator();
        assertTrue(copyingIterator.hasNext());
        ByteBuffer copy = copyingIterator.next();
        assertEquals((byte) 1, view.get(0));

        appendOnlyLog.clear();
        assertEquals((byte) 0, view.get(0)); // the log has been cleared, so the view changes
        assertEquals((byte) 1, copy.get(0)); // the log has been cleared, but the independent copy is unchanged
    }

    @Test
    public void testPaddingConfig() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        assertFalse(appendOnlyLog.isEffectivelyPadded());
        assertFalse(appendOnlyLog.isPaddingRequested());

        // check that settings persist
        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        assertFalse(appendOnlyLog.isEffectivelyPadded());
        assertFalse(appendOnlyLog.isPaddingRequested());

        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, true, true);
        assertFalse(appendOnlyLog.isEffectivelyPadded());
        assertTrue(appendOnlyLog.isPaddingRequested());
        appendOnlyLog.clear();
        assertTrue(appendOnlyLog.isEffectivelyPadded());
        assertTrue(appendOnlyLog.isPaddingRequested());

        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, true, true);
        assertTrue(appendOnlyLog.isEffectivelyPadded());
        assertTrue(appendOnlyLog.isPaddingRequested());
    }

    @Test
    public void testOrderingConfig() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        assertTrue(appendOnlyLog.isEffectiveLinearOrdering());
        assertTrue(appendOnlyLog.isRequestedLinearOrdering());

        // check that settings persist
        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        assertTrue(appendOnlyLog.isEffectiveLinearOrdering());
        assertTrue(appendOnlyLog.isRequestedLinearOrdering());

        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, false);
        assertTrue(appendOnlyLog.isEffectiveLinearOrdering());
        assertFalse(appendOnlyLog.isRequestedLinearOrdering());
        appendOnlyLog.clear();
        assertFalse(appendOnlyLog.isEffectiveLinearOrdering());
        assertFalse(appendOnlyLog.isRequestedLinearOrdering());

        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, false);
        assertFalse(appendOnlyLog.isEffectiveLinearOrdering());
        assertFalse(appendOnlyLog.isRequestedLinearOrdering());
    }

    @Test
    public void testCheckpoint() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        appendOnlyLog.put(new byte[10]);
        appendOnlyLog.checkpoint();
        appendOnlyLog.put(new byte[10]);

        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);

        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testChecksumCorruption() throws Exception {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        byte[] data = new byte[]{(byte) 1};
        appendOnlyLog.put(data);

        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        assertTrue(iter.hasNext());

        Field f = AppendOnlyLog.class.getDeclaredField("buffer");
        f.setAccessible(true);
        MappedByteBuffer buffer = (MappedByteBuffer) f.get(appendOnlyLog);

        ExecutionTracer.INSTANCE.allowNonFlushingOfDirtyLines = true;
        buffer.putInt(buffer.position() - 4, 0); // overwrite the payload to cause checksum mismatch

        iter = appendOnlyLog.iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testEmptyWrite() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);

        int remaining = appendOnlyLog.remaining();
        appendOnlyLog.put(new byte[0]);

        assertEquals(remaining, appendOnlyLog.remaining(), "empty write should be null-op");
    }

    @Test
    public void testWriteTooLarge() {

        final AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        int remaining = appendOnlyLog.remaining();
        byte[] data = new byte[remaining + 10];

        assertThrows(BufferOverflowException.class, () -> appendOnlyLog.put(data));
        assertFalse(appendOnlyLog.tryPut(data));
        assertEquals(remaining, appendOnlyLog.remaining(), "failed write should not change log content");

        assertThrows(BufferOverflowException.class, () -> appendOnlyLog.put(data, 0, data.length));
        assertFalse(appendOnlyLog.tryPut(data, 0, data.length));
        assertEquals(remaining, appendOnlyLog.remaining(), "failed write should not change log content");

        assertThrows(BufferOverflowException.class, () -> appendOnlyLog.put(ByteBuffer.wrap(data)));
        assertFalse(appendOnlyLog.tryPut(ByteBuffer.wrap(data)));
        assertEquals(remaining, appendOnlyLog.remaining(), "failed write should not change log content");
    }

    @Test
    public void testNonLinearWrite() {
        final AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, false);

        byte[] data = new byte[80];  // more than one cache line in size

        appendOnlyLog.put(data);

        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        assertEquals(data.length, iter.next().remaining());
    }


    @Test
    public void testPadding() {

        final AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);
        assertTrue(appendOnlyLog.canAccept(appendOnlyLog.remaining() - 8));

        forceInvalidation();

        final AppendOnlyLog paddedAppendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, true, true);
        assertFalse(paddedAppendOnlyLog.canAccept(paddedAppendOnlyLog.remaining() - 10));
    }


    @Test
    public void testRecovery() {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);

        int numEntriesWritten = 0;
        int i = 10;
        while (appendOnlyLog.canAccept(i)) {
            byte[] data = new byte[i];
            for (int j = 0; j < data.length; j++) {
                data[j] = (byte) i;
            }
            appendOnlyLog.put(data);
            i++;
            numEntriesWritten++;
        }

        appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, false, true);

        int numEntriesRead = 0;
        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        int expectedX = 10;
        while (iter.hasNext()) {
            ByteBuffer x = iter.next();

            if (expectedX != x.limit()) {
                throw new IllegalStateException();
            }

            for (i = 0; i < expectedX; i++) {
                if (x.get(i) != (byte) expectedX) {
                    throw new IllegalStateException();
                }
            }
            numEntriesRead++;
            expectedX++;
        }

        assertEquals(numEntriesWritten, numEntriesRead);
    }

    @ParameterizedTest
    // sadly JUnit5 doesn't have a CombinatorialSource...
    @CsvSource({
            "full_array, false, false",
            "full_array, true, false",
            "full_array, false, true",
            "full_array, true, true",

            "slice_array, false, false",
            "slice_array, true, false",
            "slice_array, false, true",
            "slice_array, true, true",

            "ByteBuffer, false, false",
            "ByteBuffer, true, false",
            "ByteBuffer, false, true",
            "ByteBuffer, true, true"
    })
    public void testWriteAndReadBack(String put, boolean padding, boolean linear) {

        AppendOnlyLog appendOnlyLog = new AppendOnlyLog(mappedByteBuffer, 0, 1024, padding, linear);

        if(padding) {
            // in padded mode, the log will try to flush the unused padding range. The hw will elide that, since
            // it knows the lines are clean. But our test harness will complain unless we suppress it...
            ExecutionTracer.INSTANCE.allowFlushingOfCleanLines = true;
        }

        int numEntriesWritten = 0;
        int i = 1;
        while (appendOnlyLog.canAccept(i)) {
            byte[] data = new byte[i];
            for (int j = 0; j < data.length; j++) {
                data[j] = (byte) i;
            }

            switch (put) {
                case "full_array":
                    appendOnlyLog.put(data);
                    break;
                case "slice_array":
                    appendOnlyLog.put(data, 0, data.length);
                    break;
                case "ByteBuffer":
                    appendOnlyLog.put(ByteBuffer.wrap(data));
                    break;
            }

            i++;
            numEntriesWritten++;
        }

        int numEntriesRead = 0;
        Iterator<ByteBuffer> iter = appendOnlyLog.iterator();
        int expectedX = 1;
        while (iter.hasNext()) {
            ByteBuffer x = iter.next();

            if (expectedX != x.limit()) {
                throw new IllegalStateException();
            }

            for (i = 0; i < expectedX; i++) {
                if (x.get(i) != (byte) expectedX) {
                    throw new IllegalStateException();
                }
            }
            numEntriesRead++;
            expectedX++;
        }

        assertEquals(numEntriesWritten, numEntriesRead);
    }
}