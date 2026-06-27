#ifndef ORCREADERFACTORY_H
#define ORCREADERFACTORY_H
#include "reader/ReaderFactory.h"
#include "OrcReader.h"
#include "OrcFileOverride.hh"
#include <unordered_set>
#include <regex>
#include <unordered_map>
#include <algorithm>
#include <iterator>


namespace omniruntime::reader {

std::unique_ptr<OrcReader> Create(
    std::shared_ptr<FileContents> &contents,
    std::unique_ptr<ORCBufferInput> &&orcBufferInput,
    const std::shared_ptr<ReaderOptions> &options);

class OrcReaderFactory : public ReaderFactory {
public:
    OrcReaderFactory() : ReaderFactory() {}

    ~OrcReaderFactory() = default;

    std::unique_ptr<Reader> CreateReader(std::shared_ptr<ReaderOptions>& options) override
    {
        auto uri = options->GetUri();
        common::ReadMode readMode = options->GetReadMode();
        std::unique_ptr<::orc::InputStream> inputStream =
            readFileOverride(*uri, readMode, static_cast<uint64_t>(options->GetFilePreloadThreshold()));
        std::unique_ptr<ORCBufferInput> stream = std::make_unique<ORCBufferInput>(std::move(inputStream));
        auto* orcStream = dynamic_cast<ORCBufferInput*>(stream.get());
        if (!orcStream) {
            throw std::invalid_argument(
                "OrcReaderFactory: Invalid input types. Expected ORCBufferInput."
            );
        }

        std::unique_ptr<ORCBufferInput> derivedStream(orcStream);
        stream.release();

        std::shared_ptr<FileContents> emptyFileContents;
        auto orcReader = Create(emptyFileContents, std::move(derivedStream), options);
        std::vector<std::string> orcColumnNames = orcReader->getAllFiedsName();

        // Check if all ORC field names match _col\d+ pattern
        std::regex missingMetadataPattern("_col\\d+");
        bool isMissMetadata = true;
        for (const auto& fieldName : orcColumnNames) {
            if (!std::regex_match(fieldName, missingMetadataPattern)) {
                isMissMetadata = false;
                break;
            }
        }

        // Get Spark field names (assuming this is available from options or elsewhere)
        auto sparkAllColumnsList = options->GetAllColumnsList();
        auto originIncludedColumnsList = options->GetIncludedColumnsList();
        std::vector<std::string> sparkColumnNames(sparkAllColumnsList.begin(), sparkAllColumnsList.end());

        // Create mappings
        std::unordered_map<std::string, std::string> sparkOrcColumnsMapping;
        std::unordered_map<std::string, std::string> orcSparkColumnsMapping;

        if (isMissMetadata) {
            // Mapping by field index
            size_t minSize = std::min(sparkColumnNames.size(), orcColumnNames.size());
            for (size_t i = 0; i < minSize; i++) {
                sparkOrcColumnsMapping[sparkColumnNames[i]] = orcColumnNames[i];
                orcSparkColumnsMapping[orcColumnNames[i]] = sparkColumnNames[i];
            }
        } else {
            // Mapping by lower case
            std::unordered_map<std::string, std::string> sparkColumnLowerMapping;
            for (const auto& sparkColumnName : sparkColumnNames) {
                std::string lowerSparkColumn = sparkColumnName;
                std::transform(lowerSparkColumn.begin(), lowerSparkColumn.end(), lowerSparkColumn.begin(), ::tolower);
                sparkColumnLowerMapping[lowerSparkColumn] = sparkColumnName;
            }

            std::unordered_map<std::string, std::string> orcColumnLowerMapping;
            for (const auto& orcColumnName : orcColumnNames) {
                std::string lowerOrcColumn = orcColumnName;
                std::transform(lowerOrcColumn.begin(), lowerOrcColumn.end(), lowerOrcColumn.begin(), ::tolower);
                orcColumnLowerMapping[lowerOrcColumn] = orcColumnName;
            }

            for (const auto& entry : sparkColumnLowerMapping) {
                auto it = orcColumnLowerMapping.find(entry.first);
                if (it != orcColumnLowerMapping.end()) {
                    sparkOrcColumnsMapping[entry.second] = it->second;
                }
            }

            for (const auto& entry : orcColumnLowerMapping) {
                auto it = sparkColumnLowerMapping.find(entry.first);
                if (it != sparkColumnLowerMapping.end()) {
                    orcSparkColumnsMapping[entry.second] = it->second;
                }
            }
        }

        // Use the mappings to update included columns list
        std::list<std::string> updateIncludedColumnsList;
        for (const auto& col : originIncludedColumnsList) {
            std::string lookupName = col;
            size_t dotPos = col.find('.');
            if (dotPos != std::string::npos) {
                lookupName = col.substr(0, dotPos);
            }

            // Check if the column has a mapping
            auto mapIt = sparkOrcColumnsMapping.find(lookupName);
            if (mapIt != sparkOrcColumnsMapping.end()) {
                // Get the mapped ORC column name
                std::string orcColName = mapIt->second;

                // Replace the original column name with the mapped ORC column name
                std::string updatedColName;
                if (dotPos != std::string::npos) {
                    // For nested columns, only replace the part before the dot
                    updatedColName = orcColName + col.substr(dotPos);
                } else {
                    // For non-nested columns, use the mapped name directly
                    updatedColName = orcColName;
                }

                updateIncludedColumnsList.push_back(updatedColName);
            }
        }

        options->SetIncludedColumnsList(updateIncludedColumnsList);
        options->ParsePredicate();

        return std::move(orcReader);
    }
};
}

#endif //ORCREADERFACTORY_H
