/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <boost/algorithm/string/replace.hpp>
#include <ostream>
#include <memory>

namespace omniruntime::type {

enum SubfieldKind {
    kAllSubscripts,
    kNestedField,
    kStringSubscript,
    kLongSubscript
};

struct Separators {
    static const std::shared_ptr<Separators>& get()
    {
        static const std::shared_ptr<Separators> instance =
                std::make_shared<Separators>();
        return instance;
    }

    bool isSeparator(char c) const
    {
        return (
            c == closeBracket || c == dot || c == openBracket || c == quote ||
            c == wildCard);
    }

    char backSlash = '\\';
    char closeBracket = ']';
    char dot = '.';
    char openBracket = '[';
    char quote = '\"';
    char wildCard = '*';
    char unicodeCaret = '^';
};

class Subfield {
public:
    class PathElement {
    public:
        virtual ~PathElement() = default;
        virtual SubfieldKind kind() const = 0;
        virtual bool isSubscript() const = 0;
        virtual std::string toString() const = 0;
        virtual size_t hash() const = 0;
        virtual bool operator==(const PathElement& other) const = 0;
        virtual std::unique_ptr<PathElement> clone() = 0;
    };

    class NestedField final : public PathElement {
    public:
        explicit NestedField(const std::string& name) : name_(name) {}

        SubfieldKind kind() const override
        {
            return kNestedField;
        }

        const std::string& name() const
        {
            return name_;
        }

        bool operator==(const PathElement& other) const override
        {
            if (this == &other) {
                return true;
            }
            return other.kind() == kNestedField &&
                   reinterpret_cast<const NestedField*>(&other)->name_ == name_;
        }

        size_t hash() const override
        {
            std::hash<std::string> hash;
            return hash(name_);
        }

        std::string toString() const override
        {
            return "." + name_;
        }

        bool isSubscript() const override
        {
            return false;
        }

        std::unique_ptr<PathElement> clone() override
        {
            return std::make_unique<NestedField>(name_);
        }

    private:
        const std::string name_;
    };

    // Separators: the customized separators to tokenize field name.
    explicit Subfield(
        const std::string& path,
        const std::shared_ptr<Separators>& separators = Separators::get());

    explicit Subfield(std::vector<std::unique_ptr<PathElement>>&& path);

    Subfield() = default;
    const std::vector<std::unique_ptr<PathElement>>& path() const
    {
        return path_;
    }

    std::vector<std::unique_ptr<PathElement>>& path()
    {
        return path_;
    }

    bool isPrefix(const Subfield &other) const
    {
        if (path_.size() < other.path_.size()) {
            for (int i = 0; i < path_.size(); ++i) {
                if (!(*path_[i].get() == *other.path_[i].get())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    bool operator==(const Subfield &other) const
    {
        if (this == &other) {
            return true;
        }

        if (path_.size() != other.path_.size()) {
            return false;
        }
        for (int i = 0; i < path_.size(); ++i) {
            if (!(*path_[i].get() == *other.path_[i].get())) {
                return false;
            }
        }
        return true;
    }

    bool valid() const
    {
        return !path_.empty() && path_[0]->kind() == kNestedField;
    }

    Subfield clone() const;

private:
    std::vector<std::unique_ptr<PathElement>> path_;
};

};
