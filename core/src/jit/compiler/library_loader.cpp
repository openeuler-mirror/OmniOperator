/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "library_loader.h"

using namespace std;

// filesystem api is not standard in C++14
namespace fs = llvm::sys::fs;

LibraryVersion::LibraryVersion(int32_t major, int32_t minor) : majorVer(major), minorVer(minor) {}

LibraryVersion::~LibraryVersion() {}

int32_t LibraryVersion::GetMajor() const
{
    return majorVer;
}
int32_t LibraryVersion::GetMinor() const
{
    return minorVer;
}
int32_t LibraryVersion::Compare(const LibraryVersion& other) const
{
    if (majorVer == other.majorVer && minorVer == other.minorVer) {
        return 0;
    }
    if (majorVer == other.majorVer && minorVer < other.minorVer || majorVer < other.majorVer) {
        return -1;
    }
    return 1;
}
bool LibraryVersion::Valid() const
{
    return majorVer != -1 && minorVer != -1;
}

CoreLibrary::CoreLibrary(std::string fileName, std::string libName, LibraryVersion(*versionCheck)(std::string),
    int32_t minMajor, int32_t minMinor) : fileName(fileName), libName(libName), versionCheck(versionCheck),
    minReq(LibraryVersion(minMajor, minMinor))
{}

CoreLibrary::~CoreLibrary() {}

LibraryVersion CoreLibrary::Check(std::string path) const
{
    return versionCheck(path);
}
std::string CoreLibrary::Name() const
{
    return libName;
}
std::string CoreLibrary::File() const
{
    return fileName;
}
LibraryVersion& CoreLibrary::GetMin()
{
    return minReq;
}

LibraryVersion GetVersionVector(string path)
{
    // Find version from getMajorVersion and getMinorVersion symbols
    void* handle;
    int32_t (*getMajor)();
    int32_t (*getMinor)();
    StringOrNull error;
    handle = dlopen(path.c_str(), RTLD_LAZY);
    if (!handle) {
        cerr << "Failed to open library: " << dlerror() << endl;
        return false;
    }
    // Clear errors
    dlerror();
    void *getMajorSym = dlsym(handle, "GetMajorVersion");
    auto getMajorCp = memcpy_s(&getMajor, sizeof(getMajor), &getMajorSym, sizeof(getMajorSym));
    if (getMajorCp != EOK) {
        cerr << "Could not copy vector GetMajorVersion function pointer" << endl;
        return false;
    }
    if ((error = dlerror()).isPresent())  {
        cerr << "Failed to find getMajorVersion method: " << error.msg() << endl;
        dlclose(handle);
        return false;
    }
    dlerror();
    void *getMinorSym = dlsym(handle, "GetMinorVersion");
    auto getMinorCp = memcpy_s(&getMinor, sizeof(getMinor), &getMinorSym, sizeof(getMinorSym));
    if (getMinorCp != EOK) {
        cerr << "Could not copy vector GetMinorVersion function pointer" << endl;
        return false;
    }
    if ((error = dlerror()).isPresent())  {
        cerr << "Failed to find getMinorVersion method: " << error.msg() << endl;
        dlclose(handle);
        return false;
    }
    int32_t detectedMajor = getMajor();
    int32_t detectedMinor = getMinor();
    int32_t res = dlclose(handle);
    if (res != 0 && (error = dlerror()).isPresent()) {
        cerr << "Failed to unload the library: " << error.msg() << endl;
        return false;
    }
    return LibraryVersion(detectedMajor, detectedMinor);
}

LibraryVersion GetVersionStdcpp(string path)
{
    // Find version from file name
    int32_t ext = path.rfind(".so");
    const int extensionLength = 4;
    int32_t majorTerminator = path.find(".", ext + extensionLength);
    // If version is missing just assume its valid
    if (majorTerminator == string::npos) {
        return LibraryVersion(0, 0);
    }
    int32_t minorTerminator = path.find(".", majorTerminator + 1);
    string slice1 = path.substr(ext + extensionLength, majorTerminator - ext - extensionLength);
    int32_t major = atoi(slice1.c_str());
    string slice2 = path.substr(majorTerminator + 1, minorTerminator - majorTerminator);
    int32_t minor = (minorTerminator == string::npos) ? 0 : atoi(slice2.c_str());
    return LibraryVersion(major, minor);
}

LibraryVersion ParseVersionJemalloc(void *&detectedVersion)
{
    if (detectedVersion == nullptr) {
        return LibraryVersion();
    }
    string versionString = static_cast<const char*>(detectedVersion);
    int32_t majorTerminator = versionString.find(".");
    if (majorTerminator == string::npos) {
        return LibraryVersion();
    }
    int32_t major = atoi(versionString.substr(0, majorTerminator).c_str());
    int32_t minorTerminator = versionString.find(".", majorTerminator + 1);
    if (minorTerminator == string::npos) {
        return LibraryVersion(major);
    }
    int32_t minor = atoi(versionString.substr(majorTerminator + 1, majorTerminator - minorTerminator).c_str());
    return LibraryVersion(major, minor);
}

void CopyString(int8_t*& target, string source)
{
    int32_t cpyIdx = 0;
    while (cpyIdx < source.size()) {
        target[cpyIdx] = source.at(cpyIdx);
        cpyIdx++;
    }
    target[cpyIdx] = '\0';
}

LibraryVersion GetVersionJemalloc(string path)
{
    // Get version from mallctl method
    void *handle;
    int32_t (*loadedMallctl)(int64_t, int64_t, int64_t, int64_t, size_t);
    StringOrNull error;
    handle = dlopen(path.c_str(), RTLD_LAZY);
    if (!handle) {
        cerr << "Failed to open library: " << dlerror() << endl;
        return false;
    }
    // Clear errors
    dlerror();
    void *symRes = dlsym(handle, "mallctl");
    auto cpRes = memcpy_s(&loadedMallctl, sizeof(loadedMallctl), &symRes, sizeof(symRes));
    if (cpRes != EOK) {
        cerr << "Could not copy Jemalloc mallctl function pointer" << endl;
        return false;
    }
    if ((error = dlerror()).isPresent())  {
        cerr << "Failed to find mallctl method: " << error.msg() << endl;
        dlclose(handle);
        return false;
    }
    void *detectedVersion = nullptr;
    auto s = make_unique<size_t>(sizeof(const char*)).release();
    // "version\0"
    const uint32_t versionWordLength = 8;
    auto versionLiteral = make_unique<int8_t[]>(versionWordLength).release();
    string vs = "version";
    CopyString(versionLiteral, vs);
    void *arg1 = &versionLiteral;
    auto carg1 = static_cast<int64_t*>(arg1);
    void *arg2t1 = &detectedVersion;
    void *arg2t2 = &arg2t1;
    auto carg2 = static_cast<int64_t*>(arg2t2);
    void *arg3 = &s;
    auto carg3 = static_cast<int64_t*>(arg3);
    int32_t errCode = loadedMallctl(*carg1, *carg2, *carg3, 0, 0);
    int32_t res = dlclose(handle);
    if (res != 0 && (error = dlerror()).isPresent()) {
        cerr << "Failed to unload the library: " << error.msg() << endl;
        dlclose(handle);
        return false;
    }
    if (errCode != 0) {
        cerr << "Could not call jemalloc mallctl method." << endl;
        return false;
    }
    return ParseVersionJemalloc(detectedVersion);
}

const unordered_map<string, CoreLibrary> LibraryLoader::GetTargets()
{
    const unordered_map<string, CoreLibrary> targets = {
        {
            "libvector.so",
            CoreLibrary("libvector.so", "vector", &GetVersionVector)
        },
        {
            "libjemalloc.so",
            CoreLibrary("libjemalloc.so", "jemalloc", &GetVersionJemalloc, 5, 2)
        },
        {
            "libstdc++.so",
            CoreLibrary("libstdc++.so", "stdc++", &GetVersionStdcpp)
        }
    };
    return targets;
}

string LibraryLoader::ExtractFileName(string path)
{
    int32_t idx = path.find_last_of("/");
    return path.substr(idx + 1);
}

LibraryLoader::LibraryLoader() : neededCore(), neededExtra()
{
    for (auto& pair : GetTargets()) {
        neededCore.insert(pair.second.Name());
    }

    ifstream extraDepConfig("/opt/lib/extra_dependencies.txt");
    string line;
    while (getline(extraDepConfig, line)) {
        neededExtra.push_back(line);
    }
}

LibraryLoader::~LibraryLoader() {}

vector<string> SplitPaths(string aggregate)
{
    vector<string> paths;
    int32_t start = 0;
    int32_t idx;
    while ((idx = aggregate.find(":", start)) != string::npos) {
        paths.push_back(aggregate.substr(start, idx - start));
        start = idx + 1;
    }
    paths.push_back(aggregate.substr(start));
    return paths;
}

vector<string> LibraryLoader::LoadLibraries(string allPaths, bool isCore)
{
    vector<string> paths;
    vector<string> toSearch = SplitPaths(allPaths);
    int32_t nTotal;
    if (isCore) {
        nTotal = neededCore.size();
    } else {
        nTotal = neededExtra.size();
    }
    for (auto& p : toSearch) {
        if (!SearchPath(p, paths, nTotal, isCore)) {
            break;
        }
    }
    if (paths.size() != nTotal) {
        if (isCore) {
            for (auto& lib : neededCore) {
                cerr << "Could not find core library " << lib << endl;
            }
        } else {
            for (auto& lib : neededExtra) {
                cerr << "Could not find extra library " << lib << endl;
            }
        }
    }
    return paths;
}

bool LibraryLoader::FinishedLoading()
{
    return neededCore.size() == 0 && neededExtra.size() == 0;
}

string LibraryLoader::ResolveSymlink(const string& path)
{
    llvm::SmallString<512> realPathRef;
    fs::real_path(path, realPathRef);
    string realPath = realPathRef.str().str();
    return realPath;
}

bool LibraryLoader::ValidateCoreLibrary(const string& path, const string& realPath)
{
    const auto targets = GetTargets();
    string fileName = ExtractFileName(path);
    // Limit search to .so files
    int32_t ext = fileName.rfind(".so");
    if (ext == string::npos) {
        return false;
    }
    const int extLength = 3;
    string truncated = fileName.substr(0, ext + extLength);
    if (targets.find(truncated) == targets.end()) {
        return false;
    }
    CoreLibrary lib = targets.at(truncated);
    string libName = lib.Name();
    if (neededCore.count(libName) == 0) {
        return false;
    }
    LibraryVersion parsedVersion = lib.Check(realPath);
    // If the found version is less than the minimum required version
    if (!parsedVersion.Valid() || parsedVersion.Compare(lib.GetMin()) == -1) {
        return false;
    }
    neededCore.erase(libName);
    return true;
}

bool LibraryLoader::ValidateExtraLibrary(const string& path, const string& realPath)
{
    string fileName = ExtractFileName(path);
    for (int32_t i = 0; i < neededExtra.size(); i++) {
        if (fileName.find(neededExtra[i]) != string::npos) {
            neededExtra.erase(neededExtra.begin() + i);
            return true;
        }
    }
    return false;
}

bool LibraryLoader::SearchPath(string path, vector<string>& dest, int32_t nTotal, bool isCore)
{
    error_code err;
    for (auto it = fs::recursive_directory_iterator(path, err);
        it != fs::recursive_directory_iterator(); it.increment(err)) {
        const fs::directory_entry& file = *it;
        if (file.type() != fs::file_type::regular_file && file.type() != fs::file_type::symlink_file) {
            continue;
        }
        // Resolve symlinks to destination file
        string realPath = (file.type() == fs::file_type::symlink_file) ? ResolveSymlink(file.path()) : file.path();
        if (isCore && !ValidateCoreLibrary(file.path(), realPath)) {
            continue;
        } else if (!isCore && !ValidateExtraLibrary(file.path(), realPath)) {
            continue;
        }
        dest.push_back(realPath);
        if (dest.size() >= nTotal) {
            return false;
        }
    }
    return true;
}