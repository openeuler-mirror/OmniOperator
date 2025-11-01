#ifndef ORCREADERFACTORY_H
#define ORCREADERFACTORY_H
#include "reader/ReaderFactory.h"
#include "OrcReader.h"
#include "OrcFileOverride.hh"
#include <unordered_set>


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
        std::unique_ptr<::orc::InputStream> inputStream = readFileOverride(*uri);
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
        std::unordered_set<std::string> orcColumnSet(orcColumnNames.begin(), orcColumnNames.end());

        auto originIncludedColumnsList = options->GetIncludedColumnsList();
        std::list<std::string> updateIncludedColumnsList;
        for (const auto& col : originIncludedColumnsList) {
            std::string lookupName = col;
            size_t dotPos = col.find('.');
            if (dotPos != std::string::npos) {
                lookupName = col.substr(0, dotPos);
            }

            if (orcColumnSet.find(lookupName) != orcColumnSet.end()) {
                updateIncludedColumnsList.push_back(col);
            }
        }

        options->SetIncludedColumnsList(updateIncludedColumnsList);
        options->ParsePredicate();

        return std::move(orcReader);
    }
};
}

#endif //ORCREADERFACTORY_H
