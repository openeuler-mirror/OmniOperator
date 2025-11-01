#ifndef OMNIOPERATORJIT_ORCREADER_H
#define OMNIOPERATORJIT_ORCREADER_H

#include <stdint-gcc.h>
#include <type/data_type.h>
#include <vector>
#include "reader/BufferInput.h"
#include "codegen/Options.h"
#include "reader/Reader.h"
#include "reader/ReaderFactory.h"
#include "orc/RowReader/Reader.hh"
#include "codegen/Options.h"

using ::orc::InputStream;
using ::orc::FileContents;
using ::orc::DataBuffer;
using omniruntime::type::RowType;

namespace omniruntime::reader {

class OrcRowReader : public omniruntime::reader::RowReader, public ::orc::RowReaderImpl {
public:
    OrcRowReader() = default;

    ~OrcRowReader() override = default;

    OrcRowReader(std::shared_ptr<FileContents> contents, const std::shared_ptr<ReaderOptions> &options);


    /**
          * direct read VectorBatch in next
          * @param batch the batch to push
          * @param omniTypeId the omniTypeId to push
          * @param batchLen the max row count of batch
          * @return the row size read
     */
    uint64_t Next(uint64_t size, vec::VectorPtr &result) override {};

    uint64_t NextDirect(std::vector<BaseVector *> *batch, int *omniTypeId, uint64_t batchLen) override;

    uint64_t Next(std::vector<BaseVector *> **batch, int *omniTypeId, uint64_t batchLen) override;

    void StartNextStripe();

private:
    std::shared_ptr <FileContents> contents_;
    std::vector<BaseVector *> *batch;
    int *omniTypeId;
};

class OrcReader : public omniruntime::reader::Reader, public ::orc::ReaderImpl {
public:
    OrcReader() = default;

    ~OrcReader() override = default;

    OrcReader(std::shared_ptr<::orc::FileContents> contents, const std::shared_ptr<ReaderOptions>& options,
        uint64_t fileLength, uint64_t postscriptLength)
        : ::orc::ReaderImpl(contents, options->GetOrcReaderOptions(), fileLength, postscriptLength)
    {
        options_ = options;
        contents_ = contents;
    }

    std::unique_ptr<RowReader> CreateRowReader() override
    {
        auto rowReader = std::make_unique<OrcRowReader>(contents_, options_);
        return std::move(rowReader);
    }

private:
    std::shared_ptr<FileContents> contents_;
};

}
#endif // OMNIOPERATORJIT_ORCREADER_H