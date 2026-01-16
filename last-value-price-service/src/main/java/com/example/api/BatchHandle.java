
package com.example.api;

import java.util.UUID;

/**
 * Opaque handle representing a batch run.
 * Immutable and safe to share across threads.
 */
public final class BatchHandle {
    private final UUID id;

    public BatchHandle(UUID id) {
        this.id = id;
    }

    public UUID getId() {
        return id;
    }

    @Override
    public String toString() {
        return "BatchHandle{" + "id=" + id + '}';
    }
}