#ifndef OMNIOPERATORJIT_READERFACTORY_H
#define OMNIOPERATORJIT_READERFACTORY_H

#include <memory>
#include "Reader.h"
#include "ReaderOptions.h"
#include "BufferInput.h"

using omniruntime::codegen::FileFormat;

namespace omniruntime::reader {

class ReaderFactory {
public:
    explicit ReaderFactory(FileFormat orc);
    virtual ~ReaderFactory() = default;
    virtual std::unique_ptr<Reader> CreateReader(std::shared_ptr<ReaderOptions>& options) = 0;
    ReaderFactory() = default;
};

std::unique_ptr<ReaderFactory> GetReaderFactory(FileFormat format);

} // namespace omniruntime::codegen
#endif // OMNIOPERATORJIT_READERFACTORY_H
