/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_LIBRARY_LOADER_H__
#define __OMNI_JIT_LIBRARY_LOADER_H__

#include "llvm/Support/FileSystem.h"
#include "../../../thirdparty/huawei_secure_c/include/securec.h"

#include <link.h>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <sstream>
#include <algorithm>

class StringOrNull {
public:
    StringOrNull(std::string mssg = "", bool present = false) : mssg(mssg), present(present) {}
    StringOrNull(char* ca) {
        if (ca != nullptr) {
            this->mssg = ca;
            this->present = true;
        } else {
            this->present = false;
        }
    }
    ~StringOrNull() {}
    StringOrNull operator=(char* ca) {
        StringOrNull son;
        if (ca != nullptr) {
            son.mssg = ca;
            son.present = true;
        }
        return son;
    }
    std::string msg() {
        return mssg;
    }
    bool isPresent() {
        return present;
    }
private:
    std::string mssg;
    bool present;
};

class CoreLibrary {
public:
    CoreLibrary(std::string fileName, std::string libName, std::string preferredPath, int32_t priority);
    CoreLibrary(std::string fileName, std::string libName, int32_t priority = 0);
    ~CoreLibrary();
    std::string Name() const;
    std::string File() const;
    std::string PreferredPath() const;
    int32_t Priority() const;
    void SetPreferredPath(std::string path);
private:
    std::string fileName{};
    std::string libName{};
    std::string preferredPath{};
    int32_t priority = 0;
};

class LibraryLoader {
public:
    LibraryLoader();
    ~LibraryLoader();
    std::vector<std::string> LoadLibraries(std::string envStr);
    bool FinishedLoading();
    static std::string ExtractFileName(std::string path);
private:
    void SearchPath(std::string path, std::unordered_map<std::string, std::vector<std::string>>& candidates);
    std::string ValidateLibrary(const std::string& path, const std::string& realPath,
                                std::unordered_map<std::string, std::vector<std::string>>& candidates);

    std::vector<CoreLibrary> neededLibs{};

    static std::string ResolveSymlink(const std::string& path);
};

#endif