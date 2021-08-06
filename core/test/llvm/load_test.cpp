#include "gtest/gtest.h"
#include <vector>

#include "../../src/jit/compiler/library_loader.h"
#include "llvm/Support/DynamicLibrary.h"

using namespace std;
using namespace llvm::sys;

TEST (LoadTest, Core) {

    LibraryLoader ll;
    EXPECT_FALSE(ll.FinishedLoading());

    auto vec = ll.LoadLibraries("/usr/local/lib/:/home/llvm/lib/:/opt/lib/:/usr/lib/gcc/x86_64-redhat-linux/4.8.5/:/usr/lib/gcc/x86_64-linux-gnu/7/", true);
    string err;
    EXPECT_EQ(vec.size(), 3);
    for (auto& s : vec) {
        std::cout << s << std::endl;
        EXPECT_FALSE(DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err));
    }

}

TEST (LoadTest, Extra) {

    LibraryLoader ll;
    EXPECT_FALSE(ll.FinishedLoading());

    auto vec = ll.LoadLibraries("", false);
    EXPECT_EQ(vec.size(), 0);
    string err;
    for (auto& s : vec) {
        std::cout << s << std::endl;
        EXPECT_FALSE(DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err));
    }

}

TEST (LoadTest, All) {

    LibraryLoader ll;
    EXPECT_FALSE(ll.FinishedLoading());

    auto vec = ll.LoadLibraries("/usr/local/lib/:/home/llvm/lib/:/opt/lib/:/usr/lib/gcc/x86_64-redhat-linux/4.8.5/:/usr/lib/gcc/x86_64-linux-gnu/7/", true);
    string err;
    for (auto& s : vec) {
        std::cout << s << std::endl;
        EXPECT_FALSE(DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err));
    }

    vec = ll.LoadLibraries("", false);
    for (auto& s : vec) {
        std::cout << s << std::endl;
        EXPECT_FALSE(DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err));
    }
    EXPECT_TRUE(ll.FinishedLoading());

}