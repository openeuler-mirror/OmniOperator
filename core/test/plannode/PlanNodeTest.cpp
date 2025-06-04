/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include <gtest/gtest.h>

#include "plannode/planNode.h"

using namespace ::omniruntime;

TEST(TestPlanNode, sortOrder) {
    struct {
        SortOrderInfo order1;
        SortOrderInfo order2;
        int expectedEqual;

        std::string debugString() const {
            return Format(
                "order1 {} order2 {} expectedEqual {}",
                order1.ToString(),
                order2.ToString(),
                expectedEqual);
        }
    } testSettings[] = {
            {{true, true}, {true, true}, true},
            {{true, false}, {true, false}, true},
            {{false, true}, {false, true}, true},
            {{false, false}, {false, false}, true},
            {{true, true}, {true, false}, false},
            {{true, true}, {false, false}, false},
            {{true, true}, {false, true}, false},
            {{true, false}, {false, false}, false},
            {{true, false}, {false, true}, false},
            {{false, true}, {false, false}, false}};
    for (const auto &testData : testSettings) {
        SCOPED_TRACE(testData.debugString());
        if (testData.expectedEqual) {
            ASSERT_EQ(testData.order1, testData.order2);
        } else {
            ASSERT_NE(testData.order1, testData.order2);
        }
    }
}