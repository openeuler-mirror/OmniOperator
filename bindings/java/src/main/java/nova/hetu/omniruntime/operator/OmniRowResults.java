/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.constants.Status;
import nova.hetu.omniruntime.vector.RowBatch;

import java.io.Closeable;

/**
 * The type Omni results.
 *
 * @since 2024-05-16
 */
public class OmniRowResults implements Closeable {
    private final RowBatch rowBatch;

    private final Status status;

    /**
     * Instantiates a new Omni results.
     *
     * @param rowBatch the vec batch
     * @param status the status
     */
    public OmniRowResults(RowBatch rowBatch, int status) {
        this.rowBatch = rowBatch;
        this.status = new Status(status);
    }

    /**
     * Get vec batches vec batch [ ].
     *
     * @return the vec batch [ ]
     */
    public RowBatch getRowBatch() {
        return rowBatch;
    }

    /**
     * Gets status.
     *
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    @Override
    public void close() {
        if (rowBatch != null) {
            rowBatch.close();
        }
    }
}
