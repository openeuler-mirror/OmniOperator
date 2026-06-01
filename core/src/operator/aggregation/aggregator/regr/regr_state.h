/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Packed state and merge helpers shared by regr_* aggregators.
 */
#ifndef OMNI_RUNTIME_REGR_STATE_H
#define OMNI_RUNTIME_REGR_STATE_H

#include <cmath>
#include <cstdint>

namespace omniruntime::op {

// Merge two partial states (ExtendedRegrAccumulator merge from Velox CovarianceAggregatesBase).
inline void RegrMerge(int64_t &count, double &meanX, double &meanY, double &c2, double &m2X, double &m2Y,
    int64_t countOther, double meanXOther, double meanYOther, double c2Other, double m2XOther, double m2YOther)
{
    if (countOther == 0)
        return;
    if (count == 0) {
        count = countOther;
        meanX = meanXOther;
        meanY = meanYOther;
        c2 = c2Other;
        m2X = m2XOther;
        m2Y = m2YOther;
        return;
    }
    int64_t newCount = count + countOther;
    double deltaMeanX = meanXOther - meanX;
    double deltaMeanY = meanYOther - meanY;
    c2 += c2Other +
        deltaMeanX * deltaMeanY * static_cast<double>(count) * static_cast<double>(countOther) /
            static_cast<double>(newCount);
    double meanXOld = meanX;
    double meanYOld = meanY;
    meanX += deltaMeanX * static_cast<double>(countOther) / static_cast<double>(newCount);
    meanY += deltaMeanY * static_cast<double>(countOther) / static_cast<double>(newCount);
    double k = 1.0 * count / static_cast<double>(newCount) * countOther;
    m2X += m2XOther + k * std::pow(meanXOld - meanXOther, 2);
    m2Y += m2YOther + k * std::pow(meanYOld - meanYOther, 2);
    count = newCount;
}

#pragma pack(push, 1)
struct RegrState {
    int64_t count{0};
    double meanX{0};
    double meanY{0};
    double c2{0};
    double m2X{0};
    double m2Y{0};

    bool IsEmpty() const { return count == 0; }
};
#pragma pack(pop)

#pragma pack(push, 1)
struct RegrReplacementState {
    int64_t n{0};
    double avg{0};
    double m2{0};

    bool IsEmpty() const { return n == 0; }
};
#pragma pack(pop)

inline void RegrReplacementMerge(int64_t &n, double &avg, double &m2,
    int64_t nOther, double avgOther, double m2Other)
{
    if (nOther == 0)
        return;
    if (n == 0) {
        n = nOther;
        avg = avgOther;
        m2 = m2Other;
        return;
    }
    int64_t newN = n + nOther;
    double delta = avgOther - avg;
    double deltaN = delta / static_cast<double>(newN);
    avg += deltaN * static_cast<double>(nOther);
    m2 = m2 + m2Other + delta * deltaN * static_cast<double>(n) * static_cast<double>(nOther);
    n = newN;
}

// --- Spark-aligned intermediate buffers (covariance / var_pop), see Spark 3.5 Covariance + VariancePop ---

#pragma pack(push, 1)
struct RegrCov4State {
    double n{0};
    double xAvg{0};
    double yAvg{0};
    double ck{0};
    bool IsEmpty() const { return n == 0.0; }
};

struct RegrVarPopState {
    double n{0};
    double avg{0};
    double m2{0};
    bool IsEmpty() const { return n == 0.0; }
};

struct RegrSlopeInterceptState {
    double covN{0};
    double covXAvg{0};
    double covYAvg{0};
    double covCk{0};
    double varN{0};
    double varAvgX{0};
    double varM2X{0};
    bool IsEmpty() const { return covN == 0.0; }
};
#pragma pack(pop)

inline void SparkCovarianceMerge(double &n1, double &x1, double &y1, double &ck1, double n2, double x2, double y2,
    double ck2)
{
    if (n2 == 0.0)
        return;
    if (n1 == 0.0) {
        n1 = n2;
        x1 = x2;
        y1 = y2;
        ck1 = ck2;
        return;
    }
    double newN = n1 + n2;
    double dx = x2 - x1;
    double dy = y2 - y1;
    double dxN = newN == 0.0 ? 0.0 : dx / newN;
    double dyN = newN == 0.0 ? 0.0 : dy / newN;
    double newXAvg = x1 + dxN * n2;
    double newYAvg = y1 + dyN * n2;
    double newCk = ck1 + ck2 + dx * dyN * n1 * n2;
    n1 = newN;
    x1 = newXAvg;
    y1 = newYAvg;
    ck1 = newCk;
}

inline void SparkCovarianceUpdate(double &n, double &xAvg, double &yAvg, double &ck, double x, double y)
{
    double newN = n + 1.0;
    double dx = x - xAvg;
    double dy = y - yAvg;
    double dyN = dy / newN;
    double newXAvg = xAvg + dx / newN;
    double newYAvg = yAvg + dyN;
    double newCk = ck + dx * (y - newYAvg);
    n = newN;
    xAvg = newXAvg;
    yAvg = newYAvg;
    ck = newCk;
}

inline void RegrVarPopMerge(double &n1, double &avg1, double &m2_1, double n2, double avg2, double m2_2)
{
    if (n2 == 0.0)
        return;
    if (n1 == 0.0) {
        n1 = n2;
        avg1 = avg2;
        m2_1 = m2_2;
        return;
    }
    double newN = n1 + n2;
    double delta = avg2 - avg1;
    double deltaN = newN == 0.0 ? 0.0 : delta / newN;
    avg1 += deltaN * n2;
    m2_1 = m2_1 + m2_2 + delta * deltaN * n1 * n2;
    n1 = newN;
}

inline void RegrVarPopUpdate(double &n, double &avg, double &m2, double x)
{
    double newN = n + 1.0;
    double delta = x - avg;
    double newAvg = avg + delta / newN;
    m2 += delta * (x - newAvg);
    n = newN;
    avg = newAvg;
}

inline void RegrSlopeInterceptUpdate(RegrSlopeInterceptState *acc, double x, double y)
{
    double newN = acc->covN + 1.0;
    double dx = x - acc->covXAvg;
    double dy = y - acc->covYAvg;
    double dxN = dx / newN;
    double dyN = dy / newN;
    double newXAvg = acc->covXAvg + dxN;
    double newYAvg = acc->covYAvg + dyN;

    acc->covCk += dx * (y - newYAvg);
    acc->varM2X += dx * (x - newXAvg);
    acc->covN = newN;
    acc->covXAvg = newXAvg;
    acc->covYAvg = newYAvg;
    acc->varN = newN;
    acc->varAvgX = newXAvg;
}

inline void RegrSlopeInterceptMergePartial(RegrSlopeInterceptState *acc, double inCovN, double inCovX, double inCovY,
    double inCk, double inVarN, double inVarAvgX, double inVarM2)
{
    if (inCovN == 0.0)
        return;
    if (acc->covN == 0.0) {
        acc->covN = inCovN;
        acc->covXAvg = inCovX;
        acc->covYAvg = inCovY;
        acc->covCk = inCk;
        acc->varN = inVarN;
        acc->varAvgX = inVarAvgX;
        acc->varM2X = inVarM2;
        return;
    }

    double n1 = acc->covN;
    double n2 = inCovN;
    double newN = n1 + n2;
    double dx = inCovX - acc->covXAvg;
    double dy = inCovY - acc->covYAvg;
    double dxN = newN == 0.0 ? 0.0 : dx / newN;
    double dyN = newN == 0.0 ? 0.0 : dy / newN;
    double newXAvg = acc->covXAvg + dxN * n2;
    double newYAvg = acc->covYAvg + dyN * n2;

    acc->covCk = acc->covCk + inCk + dx * dyN * n1 * n2;
    acc->varM2X = acc->varM2X + inVarM2 + dx * dxN * n1 * n2;
    acc->covN = newN;
    acc->covXAvg = newXAvg;
    acc->covYAvg = newYAvg;
    acc->varN = newN;
    acc->varAvgX = newXAvg;
}

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_REGR_STATE_H
