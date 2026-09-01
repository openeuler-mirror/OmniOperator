#include "ReaderFactory.h"

#include "reader/parquet/ParquetReaderFactory.h"
#include "reader/orc/OrcReaderFactory.h"
#include "reader/text/TextReaderFactory.h"

namespace omniruntime::reader {

std::unique_ptr<ReaderFactory> GetReaderFactory(FileFormat format)
{
    switch (format) {
    case FileFormat::ORC: {
            return std::make_unique<OrcReaderFactory>();
    }
    case FileFormat::PARQUET: {
            return std::make_unique<ParquetReaderFactory>();
    }
    case FileFormat::TEXT: {
            return std::make_unique<text::TextReaderFactory>();
    }
    default: {
            throw std::runtime_error("Unsupported format");
            break;
    }
    }
}

} // namespace omniruntime::reader
