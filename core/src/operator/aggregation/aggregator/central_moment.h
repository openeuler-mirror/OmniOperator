/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: CentralMomentState Ops
 */
#ifndef OMNI_RUNTIME_CENTRAL_MOMENT_H
#define OMNI_RUNTIME_CENTRAL_MOMENT_H

#include <cstdint>
#include "typed_aggregator.h"

namespace omniruntime {
    namespace op {
#pragma pack(push, 1)

        template<typename Value>
        struct CentralMomentState {
            uint64_t count = 0;
            Value centralMoment1 = 0.0;
            Value centralMoment2 = 0.0;
            Value centralMoment3 = 0.0;
            Value centralMoment4 = 0.0;
            long momentOrder = 0;
        };

#pragma pack(pop)

        template<typename MomentValue, typename InputValue>
        SIMD_ALWAYS_INLINE void
        UpdateCentralMomentOp(uint64_t *count, MomentValue *centralMoment1, MomentValue *centralMoment2,
                              MomentValue *centralMoment3, MomentValue *centralMoment4, long *momentOrder,
                              AggValueState &flag,
                              const InputValue &value) {
            MomentValue oldCount = static_cast<MomentValue>(*count);
            MomentValue oldCentralMoment1 = *centralMoment1;
            MomentValue oldCentralMoment2 = *centralMoment2;
            MomentValue oldCentralMoment3 = *centralMoment3;
            MomentValue oldCentralMoment4 = *centralMoment4;
            MomentValue newCount = oldCount + 1;
            MomentValue delta = value - oldCentralMoment1;
            MomentValue deltaN = delta / newCount;
            MomentValue newCentralMoment1 = oldCentralMoment1 + deltaN;
            MomentValue newCentralMoment2 = oldCentralMoment2 + delta * (delta - deltaN);
            MomentValue delta2 = delta * delta;
            MomentValue deltaN2 = deltaN * deltaN;
            MomentValue newCentralMoment3 = 0.0;
            MomentValue newCentralMoment4 = 0.0;
            if (*momentOrder >= 3) {
                newCentralMoment3 = oldCentralMoment3 - 3.0 * deltaN * newCentralMoment2 + delta * (delta2 - deltaN2);
            }
            if (*momentOrder >= 4) {
                newCentralMoment4 = oldCentralMoment4 - 4.0 * deltaN * newCentralMoment3 -
                                    6.0 * deltaN2 * newCentralMoment2 + delta * (delta * delta2 - deltaN * deltaN2);
            }

            *count = newCount;
            *centralMoment1 = newCentralMoment1;
            *centralMoment2 = newCentralMoment2;
            *centralMoment3 = newCentralMoment3;
            *centralMoment4 = newCentralMoment4;
            flag = AggValueState::NORMAL;
        }

        template<typename MomentValue, typename InputValue, bool addIf>
        SIMD_ALWAYS_INLINE void
        UpdateCentralMomentConditionalOp(uint64_t *count, MomentValue *centralMoment1, MomentValue *centralMoment2,
                                         MomentValue *centralMoment3, MomentValue *centralMoment4, long *momentOrder,
                                         AggValueState &flag,
                                         const InputValue &value, const uint8_t &condition) {
            if (condition == addIf) {
                UpdateCentralMomentOp<MomentValue>(count, centralMoment1, centralMoment2, centralMoment3,
                                                   centralMoment4,
                                                   momentOrder, flag, value);
            }
        }

        template<typename MomentValue>
        SIMD_ALWAYS_INLINE void
        MergeCentralMomentOp(uint64_t *count, MomentValue *centralMoment1,
                             MomentValue *centralMoment2,
                             MomentValue *centralMoment3, MomentValue *centralMoment4, long *momentOrder,
                             AggValueState &flag, const MomentValue &otherCount,
                             const MomentValue &otherCentralMoment1,
                             const MomentValue &otherCentralMoment2,
                             const MomentValue &otherCentralMoment3, const MomentValue &otherCentralMoment4) {
            if (*count == 0) {
                *count = otherCount;
                *centralMoment1 = otherCentralMoment1;
                *centralMoment2 = otherCentralMoment2;
                *centralMoment3 = otherCentralMoment3;
                *centralMoment4 = otherCentralMoment4;
                return;
            }
            MomentValue n1Value = static_cast<MomentValue>(*count);
            MomentValue n2Value = otherCount;
            MomentValue newNValue = n1Value + n2Value;
            MomentValue m1 = *centralMoment1;
            MomentValue m2 = *centralMoment2;
            MomentValue m3 = *centralMoment3;
            MomentValue m4 = *centralMoment4;
            MomentValue delta = otherCentralMoment1 - m1;
            MomentValue deltaN = 0.0;
            if (newNValue > 0) {
                deltaN = delta / newNValue;
            }
            MomentValue newCentralMoment1 = m1 + deltaN * n2Value;
            MomentValue newCentralMoment2 = m2 + otherCentralMoment2 + delta * deltaN * n1Value * n2Value;
            MomentValue newCentralMoment3 = 0.0;
            if (*momentOrder >= 3) {
                newCentralMoment3 =
                        m3 + otherCentralMoment3 + deltaN * deltaN * delta * n1Value * n2Value * (n1Value - n2Value) +
                        3.0 * deltaN * (n1Value * otherCentralMoment2 - n2Value * m2);
            }
            MomentValue newCentralMoment4 = 0.0;
            if (*momentOrder >= 4) {
                newCentralMoment4 = m4 + otherCentralMoment4 + deltaN * deltaN * deltaN * delta * n1Value * n2Value *
                                    (n1Value * n1Value - n1Value * n2Value +
                                     n2Value * n2Value) +
                                    6.0 * deltaN * deltaN *
                                    (n1Value * n1Value * otherCentralMoment2 + n2Value * n2Value * m2) +
                                    4.0 * deltaN * (n1Value * otherCentralMoment3 - n2Value * m3);
            }
            *count = newNValue;
            *centralMoment1 = newCentralMoment1;
            *centralMoment2 = newCentralMoment2;
            *centralMoment3 = newCentralMoment3;
            *centralMoment4 = newCentralMoment4;
            flag = AggValueState::NORMAL;
        }

        template<typename MomentValue, bool addIf>
        SIMD_ALWAYS_INLINE void
        MergeCentralMomentConditionalOp(uint64_t *count, MomentValue *centralMoment1,
                                        MomentValue *centralMoment2,
                                        MomentValue *centralMoment3, MomentValue *centralMoment4, long *momentOrder,
                                        AggValueState &flag, const MomentValue &otherCount,
                                        const MomentValue &otherCentralMoment1,
                                        const MomentValue &otherCentralMoment2,
                                        const MomentValue &otherCentralMoment3, const MomentValue &otherCentralMoment4,
                                        const uint8_t &condition) {
            if (condition == addIf) {
                MergeCentralMomentOp(count, centralMoment1, centralMoment2, centralMoment3, centralMoment4, momentOrder,
                                     flag, otherCount, otherCentralMoment1, otherCentralMoment2, otherCentralMoment3,
                                     otherCentralMoment4);
            }
        }
    }
}
#endif // OMNI_RUNTIME_CENTRAL_MOMENT_H
