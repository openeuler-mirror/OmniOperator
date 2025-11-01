#ifndef PARQUETREADERFACTORY_H
#define PARQUETREADERFACTORY_H
#include "ParquetReader.h"
#include "reader/ReaderFactory.h"
#include "reader/common/TimeRebaseInfo.h"
#include "util/omni_exception.h"

namespace omniruntime::reader
{

class ParquetReaderFactory : public ReaderFactory {
public:
    ParquetReaderFactory() : ReaderFactory() {}

    ~ParquetReaderFactory() = default;

    std::unique_ptr<Reader> CreateReader(std::shared_ptr<ReaderOptions>& options) override
    {
        auto uri = options->GetUri();

        // Get capacity for each record batch
        int64_t capacity = options->GetBatchLen();

        nlohmann::json json;
        std::unique_ptr<common::TimeRebaseInfo> rebaseInfoPtr = common::BuildTimeRebaseInfo(*(options->GetEnhancementJson()));
        std::string ugiString = options->GetUgiString();

        auto parquetReader = std::make_unique<ParquetReader>(rebaseInfoPtr, options);
        auto state = parquetReader->InitReader(*uri, capacity, ugiString);
        if (state != Status::OK()) {
            throw OmniException(state.ToString().c_str());
            return nullptr;
        }
        return std::move(parquetReader);
    }
};

}

#endif //PARQUETREADERFACTORY_H
