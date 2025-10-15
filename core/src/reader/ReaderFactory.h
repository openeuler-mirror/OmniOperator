#ifndef OMNIOPERATORJIT_READERFACTORY_H
#define OMNIOPERATORJIT_READERFACTORY_H

#include <memory>
#include "BufferInput.h"
#include "codegen/Options.h"
#include "Reader.h"

using omniruntime::codegen::FileFormat;

namespace omniruntime::reader {

class ReaderFactory {
public:
    /**
     * Constructor.
     * @param format File format this factory is designated to.
     */
    explicit ReaderFactory(FileFormat format) : format_(format) {}

    virtual ~ReaderFactory() = default;

    /**
     * Get the file format ths factory is designated to.
     */
    FileFormat GetFileFormat() const
    {
        return format_;
    }

    virtual std::unique_ptr <Reader> CreateReader(std::unique_ptr <BufferInput> stream,
      std::shared_ptr <ReaderOptions> options) = 0;

private:
    const FileFormat format_;
};
/**
 * Register a reader factory. Only a single factory can be registered
 * for each file format. An attempt to register multiple factories for
 * a single file format would cause a filure.
 * @return true
 */
bool RegisterReaderFactory(std::shared_ptr<ReaderFactory> factory);

/**
 * Unregister a reader factory for a specified file format.
 * @return true for unregistered factory and false for a
 * missing factory for the specfified format.
 */
bool UnregisterReaderFactory(FileFormat format);

std::shared_ptr<ReaderFactory> GetReaderFactory(FileFormat format);

} // namespace omniruntime::codegen
#endif // OMNIOPERATORJIT_READERFACTORY_H
