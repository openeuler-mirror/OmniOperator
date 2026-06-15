#ifndef READ_MODE_H
#define READ_MODE_H

#include "util/omni_exception.h"

namespace common {
enum class ReadMode {
    POSITION_READ,
    SEEK_AND_READ,
    SIZE
};

ReadMode ToReadMode(int32_t mode);
}
#endif