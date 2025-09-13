/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators source file
 */
#ifndef OMNI_RUNTIME_PERF_UTIL_H
#define OMNI_RUNTIME_PERF_UTIL_H

class PerfUtil {
public:
    void Init();

    void Start() const;

    void Stop() const;

    void Reset() const;

    long long GetData() const;

private:
    int fd;
    bool initialized;
};

#endif // OMNI_RUNTIME_PERF_UTIL_H
