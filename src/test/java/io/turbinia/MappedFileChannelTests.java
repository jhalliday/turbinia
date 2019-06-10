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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static org.junit.jupiter.api.Assertions.*;

@WithBytemanFrom(source = ExecutionTracer.class)
public class MappedFileChannelTests {

    private static File file = new File("/mnt/pmem/tx/test");

    private MappedFileChannel mappedFileChannel;

    @BeforeEach
    public void setUp() throws IOException {

        if (file.exists()) {
            file.delete();
        }

        mappedFileChannel = new MappedFileChannel(file, 1024);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if(mappedFileChannel != null) {
            mappedFileChannel.close();
        }
    }

    @Test
    public void testInitializationFailure() throws IOException {

        try {
            MappedFileChannel mappedFileChannel = new MappedFileChannel(new File("/tmp/bogus"), 1024);
            fail("should throw IOException");
        } catch (IOException e) {
            assertEquals("Operation not supported", e.getMessage());
        }
    }

    @Test
    public void testWriteAndReadExceedingCapacity() throws IOException {

        int dataCapacity = (int)mappedFileChannel.size();

        byte[] data = new byte[dataCapacity+10];
        int bytesWritten = mappedFileChannel.write(ByteBuffer.wrap(data));
        assertTrue(bytesWritten < data.length);
        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        mappedFileChannel.position(0);
        int bytesRead = mappedFileChannel.read(readBuffer);
        assertEquals(bytesWritten, bytesRead);
        assertEquals(bytesRead, readBuffer.position());
    }

    @ParameterizedTest
    @CsvSource({
            "relative, false",
            "absolute, false",
            "relative, true",
            "absolute, true"
    })
    public void testWriteAndReadBack(String mode, boolean recover) throws IOException {

        int numEntriesWritten = 0;
        int totalBytesWritten = 0;
        int i = 1;
        while (mappedFileChannel.size() - totalBytesWritten >= i) {
            byte[] dataArray = new byte[i];
            for (int j = 0; j < dataArray.length; j++) {
                dataArray[j] = (byte) i;
            }
            ByteBuffer dataBuffer = ByteBuffer.wrap(dataArray);

            int bytesWritten = 0;

            switch (mode) {
                case "relative":
                    assertEquals(totalBytesWritten, mappedFileChannel.position());
                    bytesWritten += mappedFileChannel.write(dataBuffer);
                    break;
                case "absolute":
                    bytesWritten += mappedFileChannel.write(dataBuffer, totalBytesWritten);
                    break;
            }

            assertEquals(bytesWritten, dataBuffer.position());

            mappedFileChannel.force(false);

            assertEquals(dataArray.length, bytesWritten);
            i++;
            numEntriesWritten++;
            totalBytesWritten += bytesWritten;
        }

        assertNotEquals(0, totalBytesWritten);

        if(recover) {
            // fragile: DO NOT close the MappedFileChannel before discarding,
            // since that closes the fileChannel, which then can't be reused.
            // That will be a problem if gc is ever fixed to do close properly...
            mappedFileChannel = new MappedFileChannel(file, 1024);
        }

        mappedFileChannel.position(0);

        int numEntriesRead = 0;
        int totalBytesRead = 0;
        for(int j = 0; j < numEntriesWritten; j++) {
            int expectedX = j+1;

            ByteBuffer x = ByteBuffer.allocate(expectedX);
            numEntriesRead++;

            int bytesRead = 0;

            switch (mode) {
                case "relative":
                    assertEquals(totalBytesRead, mappedFileChannel.position());
                    bytesRead += mappedFileChannel.read(x);
                    break;
                case "absolute":
                    bytesRead += mappedFileChannel.read(x, totalBytesRead);
                    break;
            }

            assertEquals(expectedX, bytesRead);
            totalBytesRead += bytesRead;

            for (i = 0; i < expectedX; i++) {
                if (x.get(i) != (byte) expectedX) {
                    throw new IllegalStateException();
                }
            }

        }

        assertEquals(numEntriesWritten, numEntriesRead);
        assertEquals(totalBytesWritten, totalBytesRead);
    }

    @Test
    public void testLongPositions() throws IOException {

        long absolutelyTooBig = ((long)Integer.MAX_VALUE)+1;

        assertThrows(IndexOutOfBoundsException.class, () -> mappedFileChannel.position(absolutelyTooBig));
        assertThrows(IndexOutOfBoundsException.class, () -> mappedFileChannel.read(ByteBuffer.allocate(1), absolutelyTooBig));
        assertThrows(IndexOutOfBoundsException.class, () -> mappedFileChannel.write(ByteBuffer.allocate(1), absolutelyTooBig));

        long relativelyTooBig = mappedFileChannel.size()+1;

        assertThrows(IndexOutOfBoundsException.class, () -> mappedFileChannel.position(relativelyTooBig));
        assertThrows(IndexOutOfBoundsException.class, () -> mappedFileChannel.read(ByteBuffer.allocate(1), relativelyTooBig));
        assertThrows(IndexOutOfBoundsException.class, () -> mappedFileChannel.write(ByteBuffer.allocate(1), relativelyTooBig));
    }

    @Test
    public void testExceptionWhenClosed() throws IOException {

        mappedFileChannel.close();

        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.write((ByteBuffer)null));
        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.write(null, 0));
        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.read((ByteBuffer)null));
        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.read(null, 0));

        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.position());
        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.position(0));
        assertThrows(ClosedChannelException.class, () -> mappedFileChannel.size());
    }

    @Test
    public void testUnsupportedMethods() throws IOException {

        assertThrows(IOException.class, () -> mappedFileChannel.read(new ByteBuffer[0], 0, 0));
        assertThrows(IOException.class, () -> mappedFileChannel.write(new ByteBuffer[0], 0, 0));
        assertThrows(IOException.class, () -> mappedFileChannel.truncate(0));
        assertThrows(IOException.class, () -> mappedFileChannel.transferTo(0, 0, null));
        assertThrows(IOException.class, () -> mappedFileChannel.transferFrom(null, 0, 0));
        assertThrows(IOException.class, () -> mappedFileChannel.map(null, 0, 0));
        assertThrows(IOException.class, () -> mappedFileChannel.lock(0, 0, false));
        assertThrows(IOException.class, () -> mappedFileChannel.tryLock(0, 0, false));
    }
}
