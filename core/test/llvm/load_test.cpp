#include "gtest/gtest.h"
#include <vector>

#include "../../src/jit/compiler/library_loader.h"
#include "llvm/Support/DynamicLibrary.h"
#include "../../libconfig.h"

using namespace std;
using namespace llvm::sys;

TEST(LoadTest, All)
{
    LibraryLoader ll;
    EXPECT_FALSE(ll.FinishedLoading());

    auto vec = ll.LoadLibraries("/usr/local/lib/:/home/llvm/lib/:" + GetLibPath() +
        ":/usr/lib/gcc/x86_64-redhat-linux/4.8.5/:/usr/lib/gcc/x86_64-linux-gnu/7/");
    string err;
    for (auto &s : vec) {
        std::cout << "Loading " << s << std::endl;
        bool res = DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err);
        EXPECT_FALSE(res);
        if (res) {
            std::cerr << "Error: " << err << std::endl;
        }
    }
    EXPECT_TRUE(ll.FinishedLoading());
}