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
 * @since 20210630
 */
public class OmniResults implements Closeable {
    private final VecBatch[] vecBatches;

    private final Status status;

    /**
     * Instantiates a new Omni results.
     *
     * @param vecBatches the vec batches
     * @param status the status
     */
    public OmniResults(VecBatch[] vecBatches, int status) {
        this.vecBatches = vecBatches;
        this.status = new Status(status);
    }

    /**
     * Get vec batches vec batch [ ].
     *
     * @return the vec batch [ ]
     */
    public VecBatch[] getVecBatches() {
        return vecBatches;
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
        for (VecBatch vecBatch : vecBatches) {
            vecBatch.close();
        }
    }
}
