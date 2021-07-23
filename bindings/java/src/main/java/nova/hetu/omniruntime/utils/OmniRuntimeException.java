/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

/**
 * The type Omni runtime exception.
 *
 * @since 20210630
 */
public class OmniRuntimeException extends RuntimeException {
    private final OmniErrorType errorType;

    /**
     * Instantiates a new Omni runtime exception.
     *
     * @param errorType the error type
     * @param msg the msg
     */
    public OmniRuntimeException(OmniErrorType errorType, String msg) {
        super(msg);
        this.errorType = errorType;
    }

    /**
     * Instantiates a new Omni runtime exception.
     *
     * @param errorType the error type
     * @param string the string
     * @param throwable the throwable
     */
    public OmniRuntimeException(OmniErrorType errorType, String string, Throwable throwable) {
        super(string, throwable);
        this.errorType = errorType;
    }

    /**
     * Instantiates a new Omni runtime exception.
     *
     * @param errorType the error type
     * @param throwable the throwable
     */
    public OmniRuntimeException(OmniErrorType errorType, Throwable throwable) {
        super(throwable);
        this.errorType = errorType;
    }
}
