#ifndef OMNIOPERATORJIT_READER_H
#define OMNIOPERATORJIT_READER_H
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <list>
#include <type/data_type.h>
#include "memory/memory_pool.h"
#include "vector/vector.h"
#include "vector/vector_common.h"
#include "vector/vector_helper.h"
#include "codegen/Options.h"
#include "orc/RowReader/Reader.hh"
#include "orc/MemoryPool.hh"
#include "orc/Adaptor.hh"
#include "orc/StripeStream.hh"
#include "orc/Reader.hh"
#include "reader/common/JulianGregorianRebase.h"
#include "reader/common/PredicateUtil.h"

using omniruntime::codegen::FileFormat;

namespace omniruntime::reader {

class ReaderOptions {
public:
    explicit ReaderOptions(FileFormat format) : format_(format) {}

    ReaderOptions() = default;

    virtual ~ReaderOptions() = default;

    FileFormat GetFormat() const { return format_; }

    void SetFileFormat(FileFormat format) { format_ = format; }

private:
    FileFormat format_ = FileFormat::ORC;
};

class RowReaderOptions {
public:
    explicit RowReaderOptions(FileFormat format) : format_(format) {}

    RowReaderOptions() = default;

    virtual ~RowReaderOptions() = default;

    FileFormat GetFormat() const { return format_; }

    void SetFileFormat(FileFormat format) { format_ = format; }

private:
    FileFormat format_ = FileFormat::ORC;
};

class OrcReaderOptions : public ReaderOptions {
public:
    OrcReaderOptions() : ReaderOptions(FileFormat::ORC), readerOptions_(std::make_unique<::orc::ReaderOptions>()) {}

    ~OrcReaderOptions() = default;

    explicit OrcReaderOptions(std::shared_ptr<::orc::ReaderOptions> options) : ReaderOptions(FileFormat::ORC),
        readerOptions_(options) {}

    void SetOrcReaderOptions(std::shared_ptr<::orc::ReaderOptions> options)
    {
        readerOptions_ = options;
    }

    std::shared_ptr<::orc::ReaderOptions> GetOrcReaderOptions()
    {
        return readerOptions_;
    }

    const ::orc::ReaderOptions &GetConstOrcReaderOptions() const
    {
        const ::orc::ReaderOptions &readerOptions = *readerOptions_;
        return readerOptions;
    }

private:
    std::shared_ptr<::orc::ReaderOptions> readerOptions_;
};

class OrcRowReaderOptions : public RowReaderOptions {
public:
    OrcRowReaderOptions() : RowReaderOptions(FileFormat::ORC),
        rowReaderOptions_(std::make_unique<::orc::RowReaderOptions>()) {}

    ~OrcRowReaderOptions() = default;

    explicit OrcRowReaderOptions(std::shared_ptr<::orc::RowReaderOptions> options) : RowReaderOptions(FileFormat::ORC),
        rowReaderOptions_(options) {}

    void SetOrcRowReaderOptions(std::shared_ptr<::orc::RowReaderOptions> options)
    {
        rowReaderOptions_ = options;
    }

    const ::orc::RowReaderOptions &GetOrcRowReaderOptions() const
    {
        return *rowReaderOptions_;
    }

private:
    std::shared_ptr<::orc::RowReaderOptions> rowReaderOptions_;
};

inline std::unique_ptr <ReaderOptions> CreateReaderOptions(FileFormat format)
{
    switch (format) {
        case FileFormat::ORC:
            return std::make_unique<OrcReaderOptions>();
        default:
            throw std::runtime_error("Unsupported format");
    }
}

inline std::unique_ptr <RowReaderOptions> CreateRowReaderOptions(FileFormat format)
{
    switch (format) {
        case FileFormat::ORC:
            return std::make_unique<OrcRowReaderOptions>();
        default:
            throw std::runtime_error("Unsupported format");
    }
}


class RowReader {
public:
    virtual ~RowReader() = default;

    /**
     * Fetch the next portion of rows.
     * @param size Max number of rows to read
     * @param result output vector
     * @param mutation The mutation to be applied during the read, null means no
     *  mutation
     * @return number of rows scanned in the file (including any rows filtered out
     *  or deleted in mutation), 0 if there are no more rows to read.
     */
    virtual uint64_t Next(uint64_t size, vec::VectorPtr &result) = 0;

    virtual uint64_t
    Next(std::vector<omniruntime::vec::BaseVector *> *batch, int *omniTypeId, uint64_t batchLen) = 0;

    virtual std::unique_ptr<common::JulianGregorianRebase>& GetJulianPtr() {};

    virtual std::unique_ptr<common::JulianGregorianRebaseDays>& GetJulianDaysPtr() {};

    virtual std::unique_ptr<common::PredicateCondition>& GetPredicatePtr() {};
};

class Reader {
public:
    Reader() = default;

    virtual std::unique_ptr <RowReader> CreateRowReader(
        std::shared_ptr <RowReaderOptions> options,
        std::unique_ptr<common::JulianGregorianRebase> &julianPtr,
        std::unique_ptr<common::PredicateCondition> &predicate) {};

    virtual ~Reader() = default;
};
}
#endif // OMNIOPERATORJIT_READER_H
