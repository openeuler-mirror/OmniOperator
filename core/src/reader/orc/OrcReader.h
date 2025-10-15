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
using ::orc::RowReader;
using ::orc::Reader;

namespace omniruntime::reader {

class OrcRowReader : public omniruntime::reader::RowReader, public ::orc::RowReaderImpl {
public:
    OrcRowReader() = default;

    OrcRowReader(std::shared_ptr<::orc::FileContents> contents, const OrcRowReaderOptions &options)
        : ::orc::RowReaderImpl(contents, options.GetOrcRowReaderOptions())
    {
        contents_ = contents;
    }

    OrcRowReader(std::shared_ptr<::orc::FileContents> contents, const ::orc::RowReaderOptions &options,
        std::unique_ptr<common::JulianGregorianRebase> &julianPtr,
        std::unique_ptr<common::PredicateCondition> &predicate)
        : ::orc::RowReaderImpl(contents, options), julianPtr(std::move(julianPtr)), predicatePtr(std::move(predicate))
    {
        contents_ = contents;
        julianDaysPtr = std::make_unique<common::JulianGregorianRebaseDays>();
    }

    /**
          * direct read VectorBatch in next
          * @param batch the batch to push
          * @param omniTypeId the omniTypeId to push
          * @param batchLen the max row count of batch
          * @return the row size read
     */

    uint64_t Next(uint64_t size, vec::VectorPtr &result) override {};

    uint64_t Next(std::vector<omniruntime::vec::BaseVector *> *batch, int *omniTypeId, uint64_t batchLen) override;

    void SetFormatRowReader(std::unique_ptr<::orc::RowReader> orc_row_reader)
    {
        orc_row_reader = std::move(orc_row_reader);
    }

    std::unique_ptr<::orc::RowReader> GetFormatRowReader()
    {
        return std::move(orc_row_reader);
    }

    void StartNextStripe();

    ~OrcRowReader() override = default;

     std::unique_ptr<common::JulianGregorianRebase>& GetJulianPtr() override
     {
         return julianPtr;
     }

    std::unique_ptr<common::JulianGregorianRebaseDays>& GetJulianDaysPtr() override
    {
        return julianDaysPtr;
    }

     std::unique_ptr<common::PredicateCondition>& GetPredicatePtr() override
     {
         return predicatePtr;
     }

private:
    std::unique_ptr<::orc::RowReader> orc_row_reader;
    std::shared_ptr <FileContents> contents_;
    std::vector<omniruntime::vec::BaseVector *> *batch;
    int *omniTypeId;
    uint64_t batchLen;
    std::unique_ptr<common::JulianGregorianRebase> julianPtr;
    std::unique_ptr<common::JulianGregorianRebaseDays> julianDaysPtr;
    std::unique_ptr<common::PredicateCondition> predicatePtr;
};

class OrcReader : public omniruntime::reader::Reader, public ::orc::ReaderImpl {
public:
    OrcReader() = default;

    OrcReader(std::shared_ptr<::orc::FileContents> contents, const OrcReaderOptions &opts,
        uint64_t fileLength, uint64_t postscriptLength)
        : ::orc::ReaderImpl(contents, opts.GetConstOrcReaderOptions(), fileLength, postscriptLength)
    {
        contents_ = contents;
    }

    OrcReader(std::shared_ptr<::orc::FileContents> contents, const ::orc::ReaderOptions &opts,
        uint64_t fileLength, uint64_t postscriptLength)
        : ::orc::ReaderImpl(contents, opts, fileLength, postscriptLength)
    {
        contents_ = contents;
    }

    std::unique_ptr <RowReader> CreateRowReader(
        std::shared_ptr <RowReaderOptions> options,
        std::unique_ptr<common::JulianGregorianRebase> &julianPtr,
        std::unique_ptr<common::PredicateCondition> &predicate) override;

    ~OrcReader() override = default;

private:
    std::shared_ptr <FileContents> contents_;
};

std::unique_ptr<OrcReader> Create(
    std::shared_ptr <FileContents> &contents,
    std::unique_ptr <ORCBufferInput> &&orcBufferInput,
    const ::orc::ReaderOptions &options);

class OrcReaderFactory : public ReaderFactory {
public:
    OrcReaderFactory() : ReaderFactory(omniruntime::codegen::FileFormat::ORC) {}

    ~OrcReaderFactory() = default;

    std::unique_ptr <Reader> CreateReader(std::unique_ptr <BufferInput> stream,
        std::shared_ptr <ReaderOptions> options) override
    {
        auto* orcStream = dynamic_cast<ORCBufferInput*>(stream.get());
        auto* orcOptions = dynamic_cast<OrcReaderOptions*>(options.get());
        if (!orcStream || !orcOptions) {
            throw std::invalid_argument(
                "OrcReaderFactory: Invalid input types. Expected ORCBufferInput and OrcReaderOptions."
            );
        }

        std::unique_ptr<ORCBufferInput> derivedStream(orcStream);
        std::shared_ptr<::orc::ReaderOptions> orcReaderOptions = orcOptions->GetOrcReaderOptions();
        stream.release();

        std::shared_ptr<FileContents> emptyFileContents;
        auto orcReader = Create(emptyFileContents,
            std::move(derivedStream), *orcReaderOptions);

        return std::move(orcReader);
    };
};
}
#endif // OMNIOPERATORJIT_ORCREADER_H