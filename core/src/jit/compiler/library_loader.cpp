/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <llvm/Support/DynamicLibrary.h>
#include "../../../libconfig.h"
#include "util/debug.h"
#include "library_loader.h"

using namespace std;
using namespace omniruntime::LibConfig;

// filesystem api is not standard in C++14
namespace fs = llvm::sys::fs;

CoreLibrary::CoreLibrary(const std::string &fileName, const std::string &libName, const std::string &preferredPath,
    int32_t priority)
    : fileName(fileName), libName(libName), preferredPath(preferredPath), priority(priority)
{}

CoreLibrary::CoreLibrary(const std::string &fileName, const std::string &libName, int32_t priority)
    : CoreLibrary(fileName, libName, "", priority)
{}

CoreLibrary::~CoreLibrary() = default;

std::string CoreLibrary::Name() const
{
    return libName;
}

std::string CoreLibrary::File() const
{
    return fileName;
}

std::string CoreLibrary::PreferredPath() const
{
    return preferredPath;
}

int32_t CoreLibrary::Priority() const
{
    return priority;
}

void CoreLibrary::SetPreferredPath(const std::string &path)
{
    this->preferredPath = path;
}

vector<string> SplitLine(const string &input)
{
    istringstream buffer(input);
    vector<string> res { istream_iterator<std::string>(buffer), istream_iterator<std::string>() };
    return res;
}

void ParseExtraDependencies(unordered_map<string, CoreLibrary> &baseLibs, vector<CoreLibrary> &neededLibs)
{
    ifstream depConfig(GetLibPath() + "dependencies.txt");
    string line;

    while (getline(depConfig, line)) {
        vector<string> args = SplitLine(line);
        // Skip empty lines
        if (args.size() == 0) {
            continue;
        }
        string libName = args[0];
        string pathName;
        if (args.size() == 1) {
            pathName = libName;
        } else {
            pathName = args[1];
        }
        string preferredPath;
        const int32_t three = 3;
        if (args.size() == three) {
            const int32_t two = 2;
            preferredPath = args[two];
        }
        int32_t priority = 10;
        if (baseLibs.find(libName) != baseLibs.end()) {
            priority = baseLibs.at(libName).Priority();
            baseLibs.erase(libName);
        }
        CoreLibrary lib(pathName, libName, preferredPath, priority);
        neededLibs.push_back(lib);
    }

    depConfig.close();
}

LibraryLoader::LibraryLoader() noexcept : neededLibs()
{
    unordered_map<string, CoreLibrary> baseLibs = { { "operator",
        CoreLibrary("libboostkit-omniop-operator-1.0.0-aarch64.so", "operator") } };

    ParseExtraDependencies(baseLibs, neededLibs);

    for (auto &p : baseLibs) {
        neededLibs.push_back(p.second);
    }

    std::stable_sort(neededLibs.begin(), neededLibs.end(),
        [](const CoreLibrary &a, const CoreLibrary &b) { return a.Priority() < b.Priority(); });
}

LibraryLoader::~LibraryLoader() = default;

bool LibraryLoader::LoadedLibraries(const string &allPaths) noexcept
{
    using namespace llvm::sys;
    for (auto &lib : neededLibs) {
        string err;
        std::string filePath(allPaths);
        filePath.append(lib.File());
        if (DynamicLibrary::LoadLibraryPermanently(filePath.c_str(), &err)) {
            LogError("Failed to load core library at path %s since %s.", filePath.c_str(), err.c_str());
            return false;
        } else {
            LLVM_DEBUG_LOG("Successfully loaded core library at path %s", filePath.c_str());
        }
    }
    return true;
}
