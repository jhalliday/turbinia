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

import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.nio.MappedByteBuffer;

/**
 * A holder for a reference to a MappedByteBuffer backed by persistent memory,
 * via which data ranges may be flushed from cache to the persistence domain.
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2019-04
 */
public class PersistenceHandle {

    private static final XLogger logger = XLoggerFactory.getXLogger(PersistenceHandle.class);

    private final MappedByteBuffer syncBuffer;
    private final int syncBufferOffset;
    private final int length;

    public PersistenceHandle(MappedByteBuffer syncBuffer, int syncBufferOffset, int length) {
        this.syncBuffer = syncBuffer;
        this.syncBufferOffset = syncBufferOffset;
        this.length = length;
    }

    public PersistenceHandle duplicate(int offset, int length) {
        logger.entry(offset, length);

        if (length > this.length) {
            throw new IllegalArgumentException("given length of " + length + " exceeds max of " + this.length);
        }

        PersistenceHandle persistenceHandle = new PersistenceHandle(syncBuffer, syncBufferOffset + offset, length);
        logger.exit(persistenceHandle);
        return persistenceHandle;
    }

    public void persist(int from, int length) {
        logger.entry(from, length);

        if (length > this.length) {
            throw new IllegalArgumentException("given length of " + length + " exceeds max of " + this.length);
        }

        syncBuffer.force(from + syncBufferOffset, length);

        logger.exit();
    }

    public void persist() {
        logger.entry();

        persist(syncBufferOffset, length);

        logger.exit();
    }

    // non-public
    int getSyncBufferOffset() {
        return syncBufferOffset;
    }
}
