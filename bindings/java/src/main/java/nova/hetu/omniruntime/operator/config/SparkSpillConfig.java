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
    private int memUsagePctForSpillThreshold;

    /**
     * Instantiates a new spark spill config.
     */
    public SparkSpillConfig() {
        super();
        numElementsForSpillThreshold = Integer.MAX_VALUE;
        memUsagePctForSpillThreshold = 90;
    }

    /**
     * Instantiates a new spark spill config.
     *
     * @param spillPath the spill path
     * @param numElementsForSpillThreshold the num elements for spill threshold
     */
    public SparkSpillConfig(String spillPath, int numElementsForSpillThreshold) {
        this(true, spillPath, DEFAULT_MAX_SPILL_BYTES, numElementsForSpillThreshold);
        this.memUsagePctForSpillThreshold = 90; // default memory usage percentage for spill threshold
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
        super(SpillConfigId.SPILL_CONFIG_SPARK, isSpillEnabled, spillPath, maxSpillBytes, DEFAULT_WRITE_BUFFER_SIZE);
        this.numElementsForSpillThreshold = numElementsForSpillThreshold;
        this.memUsagePctForSpillThreshold = 90; // default memory usage percentage for spill threshold
    }

    /**
     * Instantiates a new spark spill config.
     *
     * @param isSpillEnabled the spill enabled
     * @param spillPath the spill path
     * @param maxSpillBytes the max spill bytes
     * @param numElementsForSpillThreshold the num elements for spill threshold
     * @param memUsagePctForSpillThreshold the memory usage percentage for spill threshold
     * @param writeBufferSize the spill write buffer size
     */
    public SparkSpillConfig(boolean isSpillEnabled, String spillPath, long maxSpillBytes,
            int numElementsForSpillThreshold, int memUsagePctForSpillThreshold, long writeBufferSize) {
        super(SpillConfigId.SPILL_CONFIG_SPARK, isSpillEnabled, spillPath, maxSpillBytes, writeBufferSize);
        this.numElementsForSpillThreshold = numElementsForSpillThreshold;
        this.memUsagePctForSpillThreshold = memUsagePctForSpillThreshold;
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

    /**
     * set the memory usage percentage for spill threshold.
     *
     * @param memUsagePctForSpillThreshold the memory usage percentage for spill
     *            threshold
     */
    public void setMemUsagePctForSpillThreshold(int memUsagePctForSpillThreshold) {
        this.memUsagePctForSpillThreshold = memUsagePctForSpillThreshold;
    }

    /**
     * get the memory usage percentage for spill threshold.
     *
     * @return the num elements for spill threshold
     */
    public int getMemUsagePctForSpillThreshold() {
        return memUsagePctForSpillThreshold;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        SparkSpillConfig that = (SparkSpillConfig) obj;
        return numElementsForSpillThreshold == that.numElementsForSpillThreshold
                && memUsagePctForSpillThreshold == that.memUsagePctForSpillThreshold;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numElementsForSpillThreshold, memUsagePctForSpillThreshold);
    }
}
