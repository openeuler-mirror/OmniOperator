/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "library_loader.h"
#include "../../../libconfig.h"
#include "../../util/debug.h"

using namespace std;

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

void CoreLibrary::SetPreferredPath(string path)
{
    this->preferredPath = path;
}

string LibraryLoader::ExtractFileName(string path)
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

LibraryLoader::LibraryLoader() : neededLibs()
{
    unordered_map<string, CoreLibrary> baseLibs = {
            { "stdc++", CoreLibrary("libstdc++.so", "stdc++") },
            { "jemalloc", CoreLibrary("libjemalloc.so", "jemalloc") },
            { "memory_pool", CoreLibrary("libmemory_pool.so", "memory_pool", 1)},
            { "omni_vector", CoreLibrary("libomni_vector.so", "omni_vector", 2)},
            {"expr_visitor", CoreLibrary("libexpr_visitor.so", "expr_visitor", 1)},
            {"expressions", CoreLibrary("libexpressions.so", "expressions", 2)},
            {"codegen", CoreLibrary("libcodegen.so", "codegen", 3)},
            {"parserhelper", CoreLibrary("libparserhelper.so", "parserhelper", 4)},
            {"parser", CoreLibrary("libparser.so", "parser", 5)},
            {"projection", CoreLibrary("libprojection.so", "projection", 6)},
            {"hash_util", CoreLibrary("libhash_util.so", "hash_util", 6)},
            {"operator_util", CoreLibrary("liboperator_util.so", "operator_util", 7)},
            {"pages_index", CoreLibrary("libpages_index.so", "pages_index", 8)},
            {"pages_hash_strategy", CoreLibrary("libpages_hash_strategy.so", "pages_hash_strategy", 8)},
            {"aggregator", CoreLibrary("libaggregator.so", "aggregator", 8)},
            {"join_hash_table", CoreLibrary("libjoin_hash_table.so", "join_hash_table", 8)},
            {"group_aggregation", CoreLibrary("libgroup_aggregation.so", "group_aggregation", 9)},
            {"non_group_aggregation", CoreLibrary("libnon_group_aggregation.so", "non_group_aggregation", 9)},
            {"hash_builder", CoreLibrary("libhash_builder.so", "hash_builder", 9)},
            {"lookup_join", CoreLibrary("liblookup_join.so", "lookup_join", 9)},
            {"sort", CoreLibrary("libsort.so", "sort", 9)},
            {"topn", CoreLibrary("libtopn.so", "topn", 9)},
            {"window_function", CoreLibrary("libwindow_function.so", "window_function", 9)},
            {"window_partition", CoreLibrary("libwindow_partition.so", "window_partition", 9)},
            {"window", CoreLibrary("libwindow.so", "window", 10)},
            {"sort_expr", CoreLibrary("libsort_expr.so", "sort_expr", 11)},
            {"hash_builder_expr", CoreLibrary("libhash_builder_expr.so", "hash_builder_expr", 11)},
            {"lookup_join_expr", CoreLibrary("liblookup_join_expr.so", "lookup_join_expr", 11)}
    };

    ParseExtraDependencies(baseLibs, neededLibs);

    for (auto &p : baseLibs) {
        neededLibs.push_back(p.second);
    }
    std::stable_sort(neededLibs.begin(), neededLibs.end(),
        [](const CoreLibrary &a, const CoreLibrary &b) { return a.Priority() < b.Priority(); });
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

static int LibCallback(struct dl_phdr_info *info, size_t size, void *data)
{
    if (info->dlpi_name == nullptr) {
        return 0;
    }
    string name = info->dlpi_name;
    if (name.length() == 0) {
        return 0;
    }
    int32_t endIdx = name.find_last_of("/");
    if (endIdx == string::npos || LibraryLoader::ExtractFileName(name).find("libstdc++.so") == string::npos) {
        return 0;
    }
    vector<string> *vec = static_cast<vector<string> *>(data);
    name = name.substr(0, endIdx);
    vec->push_back(name);
    return 0;
}

vector<string> LibraryLoader::LoadLibraries(string allPaths)
{
    unordered_map<string, vector<string>> candidates;
    for (auto &lib : neededLibs) {
        candidates.insert({ lib.File(), vector<string>() });
    }
    vector<string> paths;
    vector<string> toSearch = SplitPaths(allPaths);
    dl_iterate_phdr(LibCallback, &toSearch);
    for (auto &p : toSearch) {
        SearchPath(p, candidates);
    }

    ChooseCandidates(candidates, neededLibs, paths);

    return paths;
}

bool LibraryLoader::FinishedLoading()
{
    return neededLibs.size() == 0;
}

string LibraryLoader::ResolveSymlink(const string &path)
{
    llvm::SmallString<512> realPathRef;
    fs::real_path(path, realPathRef);
    string realPath = realPathRef.str().str();
    return realPath;
}

string LibraryLoader::ValidateLibrary(const string &path, const string &realPath,
    unordered_map<string, vector<string>> &candidates)
{
    auto targets = neededLibs;
    string fileName = ExtractFileName(path);
    // Limit search to .so files
    int32_t ext = fileName.rfind(".so");
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
        if ((fileName = ValidateLibrary(file.path(), realPath, candidates)) == "") {
            continue;
        }
        LLVM_DEBUG_LOG("Found candidate library at %s", realPath.c_str());
        candidates.at(fileName).push_back(realPath);
    }
}
