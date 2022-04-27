/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static java.util.Objects.requireNonNull;

import nova.hetu.omniruntime.operator.config.OperatorConfig;

/**
 * The class Omni jit context.
 *
 * @since 2021-09-27
 */
public class OmniJitContext {
    /**
     * The operator config.
     */
    protected final OperatorConfig operatorConfig;

    /**
     * OmniJitContext constructor.
     *
     * @param operatorConfig the operator config
     */
    public OmniJitContext(OperatorConfig operatorConfig) {
        this.operatorConfig = requireNonNull(operatorConfig, "operatorConfig is null.");
    }

    /**
     * Get the operator config.
     *
     * @return the operator config
     */
    public OperatorConfig getOperatorConfig() {
        return operatorConfig;
    }
}
