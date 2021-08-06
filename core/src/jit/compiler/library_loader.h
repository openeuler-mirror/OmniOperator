/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_LIBRARY_LOADER_H__
#define __OMNI_JIT_LIBRARY_LOADER_H__

#include "llvm/Support/FileSystem.h"
#include "../../../thirdparty/huawei_secure_c/include/securec.h"

#include <dlfcn.h>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

class LibraryVersion {
public:
    LibraryVersion(int32_t major = -1, int32_t minor = -1);
    ~LibraryVersion();
    int32_t GetMajor() const;
    int32_t GetMinor() const;
    int32_t Compare(const LibraryVersion& other) const;
    bool Valid() const;
private:
    int32_t majorVer = -1;
    int32_t minorVer = -1;
};

class CoreLibrary {
public:
    CoreLibrary(std::string fileName, std::string libName, LibraryVersion(*versionCheck)(std::string),
                int32_t minMajor = 1, int32_t minMinor = 0);
    ~CoreLibrary();
    LibraryVersion Check(std::string path) const;
    std::string Name() const;
    std::string File() const;
    LibraryVersion& GetMin();
private:
    std::string fileName{};
    std::string libName{};
    LibraryVersion(*versionCheck)(std::string);
    LibraryVersion minReq;
};

class LibraryLoader {
public:
    LibraryLoader();
    ~LibraryLoader();
    std::vector<std::string> LoadLibraries(std::string envStr, bool isCore);
    bool FinishedLoading();
private:
    bool SearchPath(std::string path, std::vector<std::string>& dest, int32_t nTotal, bool isCore);
    bool ValidateCoreLibrary(const std::string& path, const std::string& realPath);
    bool ValidateExtraLibrary(const std::string& path, const std::string& realPath);

    std::unordered_set<std::string> neededCore{};
    std::vector<std::string> neededExtra{};

    static const std::unordered_map<std::string, CoreLibrary> GetTargets();
    static std::string ExtractFileName(std::string path);
    static std::string ResolveSymlink(const std::string& path);
};

#endif