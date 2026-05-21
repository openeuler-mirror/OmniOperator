#ifndef OMNIOPERATORJIT_READER_H
#define OMNIOPERATORJIT_READER_H
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <list>
#include "orc/Adaptor.hh"
#include "ReaderOptions.h"
#include "type/data_type.h"
#include <iostream>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <stdexcept>

using omniruntime::reader::ReaderOptions;


namespace omniruntime::reader {

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

    virtual uint64_t NextDirect(std::vector<BaseVector *> *batch, int *omniTypeId, uint64_t batchLen) = 0;

    virtual uint64_t
    Next(std::vector<BaseVector *> **batch, int *omniTypeId, uint64_t batchLen) = 0;

    virtual uint64_t LastReadRowPosition() const
    {
        return 0;
    }

    std::shared_ptr<common::JulianGregorianRebase> &GetJulianPtr()
    {
        return julianPtr;
    }

    std::unique_ptr<common::JulianGregorianRebaseDays> &GetJulianDaysPtr()
    {
        return julianDaysPtr;
    }

    std::shared_ptr<common::PredicateCondition> &GetPredicatePtr()
    {
        return predicatePtr;
    }

protected:
    std::shared_ptr<ReaderOptions> options_;
    omniruntime::type::RowTypePtr rowType_;
    omniruntime::type::RowTypePtr fileRowType_;
    std::shared_ptr<common::JulianGregorianRebase> julianPtr;
    std::unique_ptr<common::JulianGregorianRebaseDays> julianDaysPtr;
    std::shared_ptr<common::PredicateCondition> predicatePtr;
};

class Reader {
public:
    Reader() = default;

    virtual std::unique_ptr<RowReader> CreateRowReader() = 0;

    virtual ~Reader();

protected:
    std::shared_ptr<ReaderOptions> options_;
};

}
#endif // OMNIOPERATORJIT_READER_H
