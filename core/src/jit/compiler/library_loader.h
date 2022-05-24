/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_LIBRARY_LOADER_H__
#define __OMNI_JIT_LIBRARY_LOADER_H__

#include <huawei_secure_c/include/securec.h>
#include <link.h>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <sstream>
#include <algorithm>
#include "llvm/Support/FileSystem.h"

class CoreLibrary {
public:
    CoreLibrary(const std::string &fileName, const std::string &libName, const std::string &preferredPath,
        int32_t priority);
    CoreLibrary(const std::string &fileName, const std::string &libName, int32_t priority = 0);
    ~CoreLibrary();
    std::string Name() const;
    std::string File() const;
    std::string PreferredPath() const;
    int32_t Priority() const;
    void SetPreferredPath(const std::string &path);

private:
    std::string fileName;
    std::string libName;
    std::string preferredPath;
    int32_t priority = 0;
};

class LibraryLoader {
public:
    LibraryLoader() noexcept;
    ~LibraryLoader();
    bool LoadedLibraries(const std::string &envStr) noexcept;

private:
    std::vector<CoreLibrary> neededLibs;
};

#endif