#include "ReadMode.h"

namespace common {

ReadMode ToReadMode(int32_t mode)
{
    if (mode < 0 || mode >= static_cast<int32_t>(ReadMode::SIZE)) {
        OMNI_FAIL("Bad ReadMode: " + std::to_string(mode));
    }
    return static_cast<ReadMode>(mode);
}
}