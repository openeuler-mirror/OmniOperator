/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator.config;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_INNER_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.util.Objects;

/**
 * operator config.
 *
 * @since 2022-04-16
 */
public class OperatorConfig {
    /**
     * NONE operator config.
     */
    public static final OperatorConfig NONE = new OperatorConfig();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private boolean isJitEnabled;

    private SpillConfig spillConfig;

    private boolean isSkipExpressionVerify;

    /**
     * Operator config default constructor.
     */
    public OperatorConfig() {
        this(true, SpillConfig.NONE, false);
    }

    /**
     * Operator config constructor.
     *
     * @param isJitEnabled whether the jit enabled
     */
    public OperatorConfig(boolean isJitEnabled) {
        this(isJitEnabled, SpillConfig.NONE, false);
    }

    /**
     * Operator config constructor.
     *
     * @param isJitEnabled whether the jit enabled
     * @param isSkipExpressionVerify whether to skip exprVerify
     */
    public OperatorConfig(boolean isJitEnabled, boolean isSkipExpressionVerify) {
        this(isJitEnabled, SpillConfig.NONE, isSkipExpressionVerify);
    }

    /**
     * Operator config constructor.
     *
     * @param isJitEnabled whether the jit enabled
     * @param spillConfig the spill config
     */
    public OperatorConfig(boolean isJitEnabled, SpillConfig spillConfig) {
        this(isJitEnabled, spillConfig, false);
    }

    /**
     * Operator config constructor.
     *
     * @param spillConfig the spill config
     */
    public OperatorConfig(SpillConfig spillConfig) {
        this(true, spillConfig, false);
    }

    /**
     * Operator config constructor.
     *
     * @param isJitEnabled whether the jit enabled
     * @param spillConfig the spill config
     * @param isSkipExpressionVerify whether to skip exprVerify
     */
    public OperatorConfig(boolean isJitEnabled, SpillConfig spillConfig, boolean isSkipExpressionVerify) {
        this.isJitEnabled = isJitEnabled;
        this.spillConfig = spillConfig;
        this.isSkipExpressionVerify = isSkipExpressionVerify;
    }

    /**
     * Get spill config.
     *
     * @return the spill config
     */
    public SpillConfig getSpillConfig() {
        return spillConfig;
    }

    /**
     * Set spill config.
     *
     * @param spillConfig the spill config
     */
    public void setSpillConfig(SpillConfig spillConfig) {
        this.spillConfig = spillConfig;
    }

    /**
     * Get the status whether jit enabled.
     *
     * @return return true if jit enabled, return false if jit disabled
     */
    public boolean isJitEnabled() {
        return isJitEnabled;
    }

    /**
     * Set jit enabled.
     *
     * @param isJitEnabled whether the jit enabled
     */
    public void setJitEnabled(boolean isJitEnabled) {
        this.isJitEnabled = isJitEnabled;
    }

    /**
     * Set skipExpressionVerify
     *
     * @param isSkipExpressionVerify whether to skip exprVerify
     */
    public void setSkipExpressionVerify(boolean isSkipExpressionVerify) {
        this.isSkipExpressionVerify = isSkipExpressionVerify;
    }

    /**
     * Get skipExpressionVerify
     *
     * @return skipExpressionVerify
     */
    public boolean isSkipExpressionVerify() {
        return isSkipExpressionVerify;
    }

    /**
     * Serialize operator config to string.
     *
     * @param operatorConfig the operator config
     * @return the string result of serialization
     */
    public static String serialize(OperatorConfig operatorConfig) {
        try {
            return OBJECT_MAPPER.writeValueAsString(operatorConfig);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Serialization failed.", e);
        }
    }

    /**
     * Deserialize string to the operator config.
     *
     * @param operatorConfigString the operator config string
     * @return the operator config of deserialization
     */
    public static OperatorConfig deserialize(String operatorConfigString) {
        try {
            return OBJECT_MAPPER.readerFor(OperatorConfig.class).readValue(operatorConfigString);
        } catch (JsonProcessingException e) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Deserialization failed.", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OperatorConfig operatorConfig = (OperatorConfig) obj;
        return isJitEnabled == operatorConfig.isJitEnabled && spillConfig.equals(operatorConfig.getSpillConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isJitEnabled, spillConfig);
    }
}
