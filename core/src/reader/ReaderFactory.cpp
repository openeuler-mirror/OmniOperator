#include "ReaderFactory.h"
#include "Reader.h"
#include "util/omni_exception.h"
#include "reader/orc/OrcReader.h"

namespace omniruntime::reader {
namespace {

using ReaderFactoriesMap = std::unordered_map <FileFormat, std::shared_ptr<ReaderFactory>>;

ReaderFactoriesMap &ReaderFactories()
{
    static ReaderFactoriesMap readerFactoriesMap;
    return readerFactoriesMap;
}

} // namespace

bool RegisterReaderFactory(std::shared_ptr <ReaderFactory> factory)
{
    [[maybe_unused]] const bool ok =
        ReaderFactories().insert({factory->GetFileFormat(), factory}).second;
    // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
    return true;
}

bool UnregisterReaderFactory(FileFormat format)
{
    auto count = ReaderFactories().erase(format);
    return count == 1;
}

std::shared_ptr <ReaderFactory> GetReaderFactory(FileFormat format)
{
    switch (format) {
        case omniruntime::codegen::FileFormat::ORC: {
            return std::make_shared<omniruntime::reader::OrcReaderFactory>();
        }
        case omniruntime::codegen::FileFormat::PARQUET: {
            throw std::runtime_error("Unsupported format PARQUET");
            break;
        }
        default: {
            throw std::runtime_error("Unsupported format");
            break;
        }
    }
}
} // namespace codegen