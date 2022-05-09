/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <vector>
#include "gtest/gtest.h"
#include "jit/compiler/library_loader.h"
#include "../../libconfig.h"

namespace omniruntime {
using namespace std;
using namespace omniruntime::LibConfig;

TEST(LoadTest, All)
{
    LibraryLoader ll;
    auto shouldOK = ll.LoadedLibraries(GetLibPath());
    EXPECT_TRUE(shouldOK);

    auto shouldFail = ll.LoadedLibraries(
        "/usr/local/lib/:/home/llvm/lib/:/usr/lib/gcc/x86_64-redhat-linux/4.8.5/:/usr/lib/gcc/x86_64-linux-gnu/7/");
    EXPECT_FALSE(shouldFail);
}
}