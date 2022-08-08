/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.config;

import java.util.Objects;

/**
 * spark spill config.
 *
 * @since 2022-04-16
 */
public class SparkSpillConfig extends SpillConfig {
    private int numElementsForSpillThreshold;

    /**
     * Instantiates a new spark spill config.
     */
    public SparkSpillConfig() {
        super();
        numElementsForSpillThreshold = Integer.MAX_VALUE;
    }

    /**
     * Instantiates a new spark spill config.
     *
     * @param spillPath the spill path
     * @param numElementsForSpillThreshold the num elements for spill threshold
     */
    public SparkSpillConfig(String spillPath, int numElementsForSpillThreshold) {
        this(true, spillPath, DEFAULT_MAX_SPILL_BYTES, numElementsForSpillThreshold);
    }

    /**
     * Instantiates a new spark spill config.
     *
     * @param isSpillEnabled the spill enabled
     * @param spillPath the spill path
     * @param maxSpillBytes the max spill bytes
     * @param numElementsForSpillThreshold the num elements for spill threshold
     */
    public SparkSpillConfig(boolean isSpillEnabled, String spillPath, long maxSpillBytes,
            int numElementsForSpillThreshold) {
        super(SpillConfigId.SPILL_CONFIG_SPARK, isSpillEnabled, spillPath, maxSpillBytes);
        this.numElementsForSpillThreshold = numElementsForSpillThreshold;
    }

    /**
     * get the num elements for spill threshold.
     *
     * @return the num elements for spill threshold
     */
    public int getNumElementsForSpillThreshold() {
        return numElementsForSpillThreshold;
    }

    /**
     * set the num elements for spill threshold.
     *
     * @param numElementsForSpillThreshold the num elements for spill threshold
     */
    public void setNumElementsForSpillThreshold(int numElementsForSpillThreshold) {
        this.numElementsForSpillThreshold = numElementsForSpillThreshold;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && numElementsForSpillThreshold == ((SparkSpillConfig) obj).numElementsForSpillThreshold;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numElementsForSpillThreshold);
    }
}
