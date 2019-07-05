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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;

import static org.junit.jupiter.api.Assertions.*;

public class PmemUtilTests {

    @Test
    public void testExceptionForBadPaths() {

        File notDir = new File("/dev/null");
        try {
            PmemUtil.isPmemSupportedFor(notDir);
            fail("expected an exception");
        } catch (IllegalArgumentException e) {
            //expected
        }

        File notAThing = new File("/bogus");
        try {
            PmemUtil.isPmemSupportedFor(notAThing);
            fail("expected an exception");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void testFailOnNonPmem() throws Exception {

        File testDir = new File("/tmp/foo");
        testDir.mkdir();
        File testFile = new File(testDir, "test");

        FileChannel fileChannel = PmemUtil.pmemChannelFor(testFile, 1024, true);
        assertNull(fileChannel);
        assertEquals(0, testDir.listFiles().length);

        testFile.delete();
    }

    @Test
    public void testSucceedOnPmem() throws Exception {

        File testDir = new File("/mnt/pmem/tx");
        testDir.mkdir();
        File testFile = new File(testDir, "test");

        FileChannel fileChannel = PmemUtil.pmemChannelFor(testFile, 1024, true);
        assertNotNull(fileChannel);
        fileChannel.close();
        assertEquals(2, testDir.listFiles().length); // file+metadata
        testFile.delete();
        ((MappedFileChannel)fileChannel).deleteMetadata();
    }

    @Test
    public void testNoCreation() throws Exception {

        File testDir = new File("/mnt/pmem/tx");
        testDir.mkdir();
        File testFile = new File(testDir, "test");

        assertFalse(testFile.exists());

        try {
            PmemUtil.pmemChannelFor(testFile, 1024, false);
            fail("expected an exception");
        } catch (FileNotFoundException e) {
            // expected
        }

        testFile.delete();
    }
}