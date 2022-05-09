/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.config;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_PARAM_ERROR;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.io.Serializable;
import java.util.Objects;

/**
 * spill config.
 *
 * @since 2022-04-16
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes(value = {@JsonSubTypes.Type(value = SparkSpillConfig.class, name = "SparkSpillConfig")})
public class SpillConfig implements Serializable {
    /**
     * NONE spill config.
     */
    public static final SpillConfig NONE = new SpillConfig(SpillConfigId.SPILL_CONFIG_NONE);

    /**
     * INVALID spill config.
     */
    public static final SpillConfig INVALID = new SpillConfig(SpillConfigId.SPILL_CONFIG_INVALID);

    /**
     * The default max spill bytes.
     */
    public static final long DEFAULT_MAX_SPILL_BYTES = 100L * (1 << 30); // 100GB

    private static final long serialVersionUID = -1420544948753374714L;

    private SpillConfigId spillConfigId;

    private boolean isSpillEnabled;

    private String spillPath;

    private long maxSpillBytes;

    /**
     * Spill config default constructor.
     */
    public SpillConfig() {
        this(SpillConfigId.SPILL_CONFIG_NONE, false, "", DEFAULT_MAX_SPILL_BYTES);
    }

    /**
     * Spill config constructor.
     *
     * @param spillConfigId the spill config id
     */
    public SpillConfig(SpillConfigId spillConfigId) {
        this(spillConfigId, false, "", DEFAULT_MAX_SPILL_BYTES);
    }

    /**
     * Spill config constructor.
     *
     * @param spillConfigId the spill config id
     * @param isSpillEnabled whether the spill enabled
     * @param spillPath the spill path
     */
    public SpillConfig(SpillConfigId spillConfigId, boolean isSpillEnabled, String spillPath) {
        this(spillConfigId, isSpillEnabled, spillPath, DEFAULT_MAX_SPILL_BYTES);
    }

    /**
     * Spill config constructor.
     *
     * @param spillConfigId the spill config id
     * @param isSpillEnabled whether the spill enabled
     * @param spillPath the spill path
     * @param maxSpillBytes the max spill bytes
     */
    public SpillConfig(SpillConfigId spillConfigId, boolean isSpillEnabled, String spillPath, long maxSpillBytes) {
        if (isSpillEnabled && (spillPath == null || spillPath.isEmpty())) {
            throw new OmniRuntimeException(OMNI_PARAM_ERROR, "Enable spill but do not config spill path.");
        }
        this.spillConfigId = spillConfigId;
        this.isSpillEnabled = isSpillEnabled;
        this.spillPath = spillPath;
        this.maxSpillBytes = maxSpillBytes;
    }

    /**
     * get the spill config id.
     *
     * @return the spill config id
     */
    public SpillConfigId getSpillConfigId() {
        return spillConfigId;
    }

    /**
     * set the spill config id.
     *
     * @param spillConfigId the spill config id
     */
    public void setSpillConfigId(SpillConfigId spillConfigId) {
        this.spillConfigId = spillConfigId;
    }

    /**
     * get whether the spill enabled.
     *
     * @return return true if enable spill, return false if disable spill
     */
    public boolean isSpillEnabled() {
        return isSpillEnabled;
    }

    /**
     * set whether spill enabled.
     *
     * @param isSpillEnabled the status of spill enabled
     */
    public void setSpillEnabled(boolean isSpillEnabled) {
        this.isSpillEnabled = isSpillEnabled;
    }

    /**
     * get the spill path.
     *
     * @return the spill path
     */
    public String getSpillPath() {
        return spillPath;
    }

    /**
     * set the spill path.
     *
     * @param spillPath the spill path
     */
    public void setSpillPath(String spillPath) {
        this.spillPath = spillPath;
    }

    /**
     * get the max spill bytes.
     *
     * @return the max spill bytes
     */
    public long getMaxSpillBytes() {
        return maxSpillBytes;
    }

    /**
     * set the max spill bytes.
     *
     * @param maxSpillBytes the max spill bytes
     */
    public void setMaxSpillBytes(long maxSpillBytes) {
        this.maxSpillBytes = maxSpillBytes;
    }

    /**
     * The enum for spill config id.
     */
    public enum SpillConfigId {
        SPILL_CONFIG_NONE,
        SPILL_CONFIG_OLK,
        SPILL_CONFIG_SPARK,
        SPILL_CONFIG_INVALID
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SpillConfig spillConfig = (SpillConfig) obj;
        return spillConfigId == spillConfig.spillConfigId && isSpillEnabled == isSpillEnabled
                && spillPath.equals(spillConfig.spillPath) && maxSpillBytes == spillConfig.maxSpillBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(spillConfigId, isSpillEnabled, spillPath, maxSpillBytes);
    }
}
