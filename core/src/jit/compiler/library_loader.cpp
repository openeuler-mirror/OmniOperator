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

CoreLibrary::CoreLibrary(std::string fileName, std::string libName, std::string preferredPath, int32_t priority)
    : fileName(fileName), libName(libName), preferredPath(preferredPath), priority(priority)
{}

CoreLibrary::CoreLibrary(std::string fileName, std::string libName, int32_t priority)
    : CoreLibrary(fileName, libName, "", priority)
{}

CoreLibrary::~CoreLibrary() {}

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

void CoreLibrary::SetPreferredPath(std::string path)
{
    this->preferredPath = path;
}

string LibraryLoader::ExtractFileName(const std::string &path)
{
    int32_t idx = path.find_last_of("/");
    return path.substr(idx + 1);
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

vector<string> SplitPaths(const string &aggregate)
{
    vector<string> paths;
    uint64_t start = 0;
    uint64_t idx = 0;
    while ((idx = aggregate.find(':', start)) != string::npos) {
        paths.push_back(aggregate.substr(start, idx - start));
        start = idx + 1;
    }
    paths.push_back(aggregate.substr(start));
    return paths;
}

bool EndsWith(const string &source, const string &suffix)
{
    if (source.length() >= suffix.length()) {
        return source.compare(source.length() - suffix.length(), suffix.length(), suffix) == 0;
    } else {
        return false;
    }
}

void ChooseCandidates(unordered_map<string, vector<string>> &candidates, vector<CoreLibrary> &neededLibs,
    vector<string> &paths)
{
    for (auto it = neededLibs.begin(); it != neededLibs.end();) {
        auto lib = *it;
        vector<string> &options = candidates.at(lib.File());
        if (options.empty()) {
            std::cerr << "Could not find any candidates for library " << lib.Name() << std::endl;
            it++;
            continue;
        }
        LLVM_DEBUG_LOG("Found %zu candidates for library %s", options.size(), lib.Name().c_str());
        if (lib.PreferredPath().empty()) {
            string option = options[0];
            LLVM_DEBUG_LOG("No preferred path, using first option: %s", option.c_str());
            paths.push_back(option);
            it = neededLibs.erase(it);
            continue;
        }
        bool found = false;
        for (auto &option : options) {
            if (EndsWith(option, lib.PreferredPath())) {
                LLVM_DEBUG_LOG("Found option matching preferred path: %s", option.c_str());
                paths.push_back(option);
                found = true;
                break;
            }
        }
        if (!found) {
            string option = options[0];
            LLVM_DEBUG_LOG("Could not find option matching preferred path, "
                "falling back to first option: %s",
                option.c_str());
            paths.push_back(option);
        }
        it = neededLibs.erase(it);
    }
}

bool LibraryLoader::LoadedLibraries(const string &allPaths) noexcept
{
    using namespace llvm::sys;
    for (auto &lib : neededLibs) {
        string err;
        std::string filePath(allPaths);
        filePath.append(lib.File());
        if (DynamicLibrary::LoadLibraryPermanently(filePath.c_str(), &err)) {
            llvm::errs() << "Failed to load core library at path " << filePath << "\n";
            llvm::errs() << err << "\n";
            return false;
        } else {
            LLVM_DEBUG_LOG("Successfully loaded core library at path %s", filePath.c_str());
            return true;
        }
    }
    return true;
}

string LibraryLoader::ResolveSymlink(const std::string &path)
{
    llvm::SmallString<512> realPathRef;
    fs::real_path(path, realPathRef);
    string realPath = realPathRef.str().str();
    return realPath;
}

string LibraryLoader::ValidateLibrary(const string &path, unordered_map<string, vector<string>> &candidates)
{
    auto targets = neededLibs;
    string fileName = ExtractFileName(path);
    // Limit search to .so files
    auto ext = fileName.rfind(".so");
    if (ext == string::npos) {
        return "";
    }
    const int extLength = 3;
    string truncated = fileName.substr(0, ext + extLength);
    CoreLibrary lib("", "");
    for (auto &target : targets) {
        if (fileName.find(target.File()) != string::npos) {
            lib = target;
            break;
        }
    }
    if (lib.Name() == "") {
        return "";
    }
    // Early return if we are going to select the first candidate anwyays
    if (lib.PreferredPath() == "" && candidates.at(lib.File()).size() > 0) {
        return "";
    }
    return lib.File();
}

void LibraryLoader::SearchPath(string path, unordered_map<string, vector<string>> &candidates)
{
    error_code err;
    for (auto it = fs::recursive_directory_iterator(path, err); it != fs::recursive_directory_iterator();
        it.increment(err)) {
        const fs::directory_entry &file = *it;
        if (file.type() != fs::file_type::regular_file && file.type() != fs::file_type::symlink_file) {
            continue;
        }
        // Resolve symlinks to destination file
        string realPath = (file.type() == fs::file_type::symlink_file) ? ResolveSymlink(file.path()) : file.path();
        string fileName;
        if ((fileName = ValidateLibrary(file.path(), candidates)) == "") {
            continue;
        }
        LLVM_DEBUG_LOG("Found candidate library at %s", realPath.c_str());
        candidates.at(fileName).push_back(realPath);
    }
}
