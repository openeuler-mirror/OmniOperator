#ifndef PARQUETREADERFACTORY_H
#define PARQUETREADERFACTORY_H
#include "ParquetReader.h"
#include "reader/ReaderFactory.h"
#include "reader/common/TimeRebaseInfo.h"
#include "util/omni_exception.h"
#include <parquet/metadata.h> // 确保包含头文件

namespace omniruntime::reader
{

class ParquetReaderFactory : public ReaderFactory {
public:
    ParquetReaderFactory() : ReaderFactory() {}

    ~ParquetReaderFactory() = default;

    std::unique_ptr<Reader> CreateReader(std::shared_ptr<ReaderOptions>& options) override
    {
        auto uri = options->GetUri();
        int64_t capacity = options->GetBatchLen();
        std::string ugiString = options->GetUgiString();

        std::unique_ptr<common::TimeRebaseInfo> rebaseInfoPtr = common::BuildTimeRebaseInfo(*(options->GetEnhancementJson()));
        auto parquetReader = std::make_unique<ParquetReader>(rebaseInfoPtr, options);
        auto state = parquetReader->InitReader(*uri, capacity, ugiString);
        if (state != Status::OK()) {
            throw OmniException(state.ToString().c_str());
        }

        std::shared_ptr<parquet::FileMetaData> metadata_ptr = parquetReader->arrow_reader->parquet_reader()->metadata();
        const parquet::FileMetaData* metadata = metadata_ptr.get();
        const parquet::SchemaDescriptor* schema = metadata->schema();

        const parquet::schema::GroupNode* group_node = schema->group_node();
        std::unordered_set<std::string> parquetColumnSet;

        for (int i = 0, fieldCnt = group_node->field_count(); i < fieldCnt; ++i) {
            auto field = group_node->field(i);
            std::string col_name = field->name();
            parquetColumnSet.insert(col_name);
        }

        auto originIncludedColumnsList = options->GetIncludedColumnsList();
        std::list<std::string> updateIncludedColumnsList;
        for (const auto& col : originIncludedColumnsList) {
            std::string lookupName = col;
            size_t dotPos = col.find('.');
            if (dotPos != std::string::npos) {
                lookupName = col.substr(0, dotPos);
            }

            if (parquetColumnSet.find(lookupName) != parquetColumnSet.end()) {
                updateIncludedColumnsList.push_back(col);
            }
        }

        options->SetIncludedColumnsList(updateIncludedColumnsList);
        options->ParseParquetPredicate();

        std::vector<std::string> filtered_parquet_cols(updateIncludedColumnsList.begin(), updateIncludedColumnsList.end());
        options->SetParquetIncludedColumns(filtered_parquet_cols);
        return std::move(parquetReader);
    }
};

}

#endif //PARQUETREADERFACTORY_H
