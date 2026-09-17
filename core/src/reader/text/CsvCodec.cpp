/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/CsvCodec.h"

#include <algorithm>
#include <stdexcept>

namespace omniruntime::reader::text {
namespace {
constexpr size_t DEFAULT_MAX_COLUMNS = 20480;
}

CsvCodec::CsvCodec(const TextFormatOptions& options)
    : options_(options.Csv()), hive_(options.sourceKind == TextSourceKind::HIVE_TEXT)
{}

CsvCodec::CsvCodec(const TextFormatOptions& options, const std::vector<int32_t>& projection)
    : CsvCodec(options)
{
    projected_ = true;
    for (size_t index = 0; index < projection.size(); ++index) {
        if (projection[index] < 0) {
            throw std::runtime_error("CSV projected field index must be non-negative.");
        }
        projection_.push_back({static_cast<size_t>(projection[index]), index});
    }
    std::stable_sort(projection_.begin(), projection_.end(),
        [](const ProjectedField& left, const ProjectedField& right) {
            return left.source < right.source;
        });
}

void CsvCodec::DecodeRecord(std::string_view record, DecodedTextRecord& output) const
{
    if (hive_) {
        DecodeHiveRecord(record, output);
        return;
    }
    output.Reset();
    // Decoded bytes cannot exceed input bytes. Reserve before exposing any views.
    if (projected_) {
        output.fields.resize(projection_.size(), TextFieldView{true, {}});
    }
    const auto delimiter = options_.delimited.fieldDelimiter;
    const auto quote = options_.quote;
    const auto escape = options_.delimited.escapeChar;
    size_t position = 0;
    size_t fieldIndex = 0;
    size_t projectedIndex = 0;
    bool more = true;
    while (more) {
        if (fieldIndex >= DEFAULT_MAX_COLUMNS) {
            throw std::runtime_error("CSV record exceeds default maxColumns (20480).");
        }
        const bool selected = !projected_ ||
            (projectedIndex < projection_.size() && projection_[projectedIndex].source == fieldIndex);
        if (options_.ignoreLeadingWhitespace) {
            while (position < record.size() && record[position] != delimiter &&
                static_cast<unsigned char>(record[position]) <= ' ') {
                ++position;
            }
        }
        bool quoted = position < record.size() && record[position] == quote;
        const bool wasQuoted = quoted;
        if (quoted) {
            ++position;
        }
        const size_t valueStart = position;
        size_t valueEnd = position;
        size_t copiedUntil = position;
        const size_t storageStart = output.storage.size();
        bool transformed = false;
        auto appendThrough = [&](size_t end) {
            if (selected && end > copiedUntil) {
                output.storage.reserve(record.size());
                output.storage.append(record.data() + copiedUntil, end - copiedUntil);
            }
        };
        while (position < record.size()) {
            const char current = record[position];
            if (current == delimiter && !quoted) {
                break;
            }
            if (quoted && current == escape && position + 1 < record.size() &&
                (record[position + 1] == quote || record[position + 1] == escape)) {
                appendThrough(position);
                if (selected) {
                    output.storage.reserve(record.size());
                    output.storage.push_back(record[position + 1]);
                }
                position += 2;
                copiedUntil = position;
                transformed = true;
                valueEnd = position;
                continue;
            }
            if (quoted && current == quote) {
                size_t following = position + 1;
                while (following < record.size() && record[following] != delimiter &&
                    static_cast<unsigned char>(record[following]) <= ' ') {
                    ++following;
                }
                if (following == record.size() || record[following] == delimiter) {
                    valueEnd = position;
                    position = following;
                    quoted = false;
                    break;
                }
                // STOP_AT_DELIMITER: an unescaped quote makes the remainder unquoted.
                // Retain the opening quote and all characters of the malformed field.
                quoted = false;
                if (selected) {
                    output.storage.reserve(record.size());
                    appendThrough(position);
                    output.storage.insert(storageStart, 1, quote);
                    output.storage.push_back(quote);
                }
                transformed = true;
                copiedUntil = ++position;
                valueEnd = position;
                continue;
            }
            ++position;
            valueEnd = position;
        }
        if (!wasQuoted && options_.ignoreTrailingWhitespace) {
            while (valueEnd > valueStart &&
                static_cast<unsigned char>(record[valueEnd - 1]) <= ' ') {
                --valueEnd;
            }
        }
        if (selected) {
            std::string_view value;
            if (transformed) {
                appendThrough(valueEnd);
                value = std::string_view(output.storage).substr(storageStart);
            } else {
                value = record.substr(valueStart, valueEnd - valueStart);
            }
            if (wasQuoted && value.empty()) {
                value = options_.emptyValue;
            }
            const bool isNull = (!wasQuoted && value.empty()) ||
                value == options_.delimited.nullLiteral;
            const TextFieldView field{isNull, value};
            if (projected_) {
                do {
                    output.fields[projection_[projectedIndex++].output] = field;
                } while (projectedIndex < projection_.size() &&
                    projection_[projectedIndex].source == fieldIndex);
            } else {
                output.fields.push_back(field);
            }
        }
        more = position < record.size();
        if (more) {
            ++position;
        }
        ++fieldIndex;
    }
}

void CsvCodec::EncodeRecord(const std::vector<TextFieldView>& fields, std::string& output) const
{
    output.clear();
    const auto delimiter = options_.delimited.fieldDelimiter;
    const auto quote = options_.quote;
    const auto escape = options_.delimited.escapeChar;
    for (size_t index = 0; index < fields.size(); ++index) {
        if (index != 0) {
            output.push_back(delimiter);
        }
        if (fields[index].isNull) {
            if (!hive_) {
                output.append(options_.delimited.nullLiteral);
            }
            continue;
        }
        auto value = fields[index].value;
        const auto originalValue = value;
        bool writeTrimmedEmptyValue = false;
        if (!hive_) {
            const bool originallyEmpty = value.empty();
            bool emptiedByLeadingTrim = false;
            if (options_.ignoreLeadingWhitespace) {
                while (!value.empty() && static_cast<unsigned char>(value.front()) <= ' ') {
                    value.remove_prefix(1);
                }
                emptiedByLeadingTrim = !originallyEmpty && value.empty();
            }
            if (options_.ignoreTrailingWhitespace) {
                while (!value.empty() && static_cast<unsigned char>(value.back()) <= ' ') {
                    value.remove_suffix(1);
                }
            }
            if (value.empty()) {
                if (originallyEmpty || emptiedByLeadingTrim) {
                    output.append(options_.emptyValue);
                    continue;
                }
                value = options_.emptyValue;
                if (value.empty()) {
                    continue;
                }
                writeTrimmedEmptyValue = true;
            }
        }
        if (writeTrimmedEmptyValue) {
            output.append(originalValue);
            for (const char byte : value) {
                if (byte == quote || byte == escape) {
                    output.push_back(escape);
                }
                output.push_back(byte);
            }
            // Match Univocity's shared appender: trailing whitespace remains at
            // the front while the same number of bytes is removed from the end.
            output.resize(output.size() - originalValue.size());
            continue;
        }
        const bool quoted = hive_ || options_.quoteAll || value.empty() ||
            value.find(delimiter) != std::string_view::npos ||
            (options_.escapeQuotes && value.find(quote) != std::string_view::npos) ||
            (!value.empty() && value.front() == quote) ||
            value.find_first_of("\r\n") != std::string_view::npos;
        if (quoted) {
            output.push_back(quote);
        }
        for (const char byte : value) {
            if (quoted && (byte == quote || byte == escape)) {
                output.push_back(escape);
            }
            output.push_back(byte);
        }
        if (quoted) {
            output.push_back(quote);
        }
    }
}

void CsvCodec::DecodeHiveRecord(std::string_view record, DecodedTextRecord& output) const
{
    output.Reset();
    if (projected_) {
        output.fields.resize(projection_.size(), TextFieldView{true, {}});
    }
    // OpenCSVSerde creates a fresh CSVReader over each Text record. An empty
    // input has no readNext result, while a missing trailing field is an empty string.
    if (record.empty()) {
        return;
    }
    const auto delimiter = options_.delimited.fieldDelimiter;
    const auto quote = options_.quote;
    const auto escape = options_.delimited.escapeChar == '"' ? '\\' : options_.delimited.escapeChar;
    size_t position = 0;
    size_t source = 0;
    size_t target = 0;
    bool more = true;
    while (more) {
        const bool selected = !projected_ ||
            (target < projection_.size() && projection_[target].source == source);
        size_t start = position;
        size_t copiedUntil = start;
        const size_t storageStart = output.storage.size();
        bool transformed = false;
        bool inQuotes = false;
        bool inField = false;
        size_t fieldEnd = std::string_view::npos;
        auto copyThrough = [&](size_t end) {
            if (selected) {
                output.storage.reserve(record.size());
                output.storage.append(record.data() + copiedUntil, end - copiedUntil);
            }
        };
        auto removeByte = [&]() {
            copyThrough(position);
            copiedUntil = position + 1;
            transformed = true;
        };
        while (position < record.size()) {
            const char byte = record[position];
            if (byte == delimiter && !inQuotes) {
                break;
            }
            if (byte == escape) {
                removeByte();
                if ((inQuotes || inField) && position + 1 < record.size() &&
                    (record[position + 1] == quote || record[position + 1] == escape)) {
                    ++position;
                }
            } else if (byte == quote) {
                if (position == start && !inQuotes && !inField) {
                    // Stripping an outer quote alone does not require a scratch copy.
                    start = ++position;
                    copiedUntil = start;
                    inQuotes = true;
                    inField = true;
                    continue;
                }
                if (inQuotes && (position + 1 == record.size() ||
                    record[position + 1] == delimiter)) {
                    fieldEnd = position++;
                    inQuotes = false;
                    break;
                }
                if ((inQuotes || inField) && position + 1 < record.size() &&
                    record[position + 1] == quote) {
                    removeByte();
                    ++position;
                } else {
                    const bool embedded = position > 2 && record[position - 1] != delimiter &&
                        position + 1 < record.size() && record[position + 1] != delimiter;
                    if (!embedded) {
                        removeByte();
                    } else if (position > start &&
                        std::all_of(record.begin() + start, record.begin() + position,
                            [](unsigned char value) {
                                return (value >= 9 && value <= 13) || (value >= 28 && value <= 32);
                            })) {
                        if (selected) {
                            output.storage.resize(storageStart);
                        }
                        start = position + 1;
                        copiedUntil = start;
                        transformed = true;
                    }
                    inQuotes = !inQuotes;
                }
                inField = !inField;
            } else {
                inField = true;
            }
            ++position;
        }
        // readNext reaches EOF with a pending quoted field: only complete preceding
        // fields are returned. Missing fields retain the preinitialized NULL entries.
        if (inQuotes) {
            return;
        }
        if (selected) {
            std::string_view value;
            const auto end = fieldEnd == std::string_view::npos ? position : fieldEnd;
            if (transformed) {
                copyThrough(end);
                value = std::string_view(output.storage).substr(storageStart);
            } else {
                value = record.substr(start, end - start);
            }
            const TextFieldView field{false, value};
            if (projected_) {
                do {
                    output.fields[projection_[target++].output] = field;
                } while (target < projection_.size() && projection_[target].source == source);
            } else {
                output.fields.push_back(field);
            }
        }
        more = position < record.size();
        if (more) {
            ++position;
        }
        ++source;
        if (projected_ && target == projection_.size()) {
            return;
        }
    }
}

} // namespace omniruntime::reader::text
