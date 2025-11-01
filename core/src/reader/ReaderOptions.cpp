#include "ReaderOptions.h"
#include "reader/jni/OrcColumnarBatchJniReader.h"

namespace  omniruntime::reader {

void ReaderOptions::ParseEnhanceJson(const std::string &enhancementJson)
{
    enhancementJson_ = std::make_shared<nlohmann::json>(nlohmann::json::parse(enhancementJson));

    if (enhancementJson_->contains("ugi")) {
        ugiString_ = enhancementJson_->at("ugi").get<std::string>();
    }

    int64_t offset = 0;
    if (enhancementJson_->contains("offset") && enhancementJson_->at("offset").is_number()) {
        offset = enhancementJson_->at("offset").get<int64_t>();
    }

    int64_t length = 0;
    if (enhancementJson_->contains("length") && enhancementJson_->at("length").is_number()) {
        length = enhancementJson_->at("length").get<int64_t>();
    }

    if (enhancementJson_->contains("includedColumns") && enhancementJson_->at("includedColumns").is_string()) {
        std::string colsStr = enhancementJson_->at("includedColumns").get<std::string>();
        std::stringstream ss(colsStr);
        std::string col;
        while (std::getline(ss, col, ',')) {
            col.erase(0, col.find_first_not_of(" \t"));
            col.erase(col.find_last_not_of(" \t") + 1);
            if (!col.empty()) {
                includedColumnsList_.push_back(col);
            }
        }
    }
}

void ReaderOptions::ParsePredicate()
{
    ParseJson(*enhancementJson_, includedColumnsList_, julianPtr_, predicatePtr_, searchArgument_);
    if (batchLen_ > 0 && predicatePtr_ != nullptr) {
        predicatePtr_->init(batchLen_);
    }
}

}
