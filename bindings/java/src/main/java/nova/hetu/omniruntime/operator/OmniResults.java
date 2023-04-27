/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.constants.Status;
import nova.hetu.omniruntime.vector.VecBatch;

import java.io.Closeable;

/**
 * The type Omni results.
 *
 * @since 2021-06-30
 */
public class OmniResults implements Closeable {
    private final VecBatch vecBatch;

    private final Status status;

    /**
     * Instantiates a new Omni results.
     *
     * @param vecBatch the vec batch
     * @param status the status
     */
    public OmniResults(VecBatch vecBatch, int status) {
        this.vecBatch = vecBatch;
        this.status = new Status(status);
    }

    /**
     * Get vec batches vec batch [ ].
     *
     * @return the vec batch [ ]
     */
    public VecBatch getVecBatch() {
        return vecBatch;
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
        if (vecBatch != null) {
            vecBatch.releaseAllVectors();
            vecBatch.close();
        }
    }
}
