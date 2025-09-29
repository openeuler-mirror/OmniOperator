/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <functional>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <optional>
#include "util/omni_exception.h"

namespace omniruntime::config {
enum class CapacityUnit {
    BYTE,
    KILOBYTE,
    MEGABYTE,
    GIGABYTE,
    TERABYTE,
    PETABYTE
};

double ToBytesPerCapacityUnit(CapacityUnit unit);

CapacityUnit ValueOfCapacityUnit(const std::string &unitStr);

/// Convert capacity string with unit to the capacity number in the specified
/// units
uint64_t ToCapacity(const std::string &from, CapacityUnit to);

std::chrono::duration<double> ToDuration(const std::string &str);

template <typename T>
T ToT(const std::string &key)
{
    T converted;
    if constexpr (std::is_same_v<bool, T>) {
        if (key == "true") {
            return true;
        } else if (key == "false") {
            return false;
        } else {
            OMNI_THROW("RUNTIME_ERROR:", "Unsupported bool value");
        }
    } else {
        std::stringstream(key) >> converted;
        return converted;
    }
}

template <typename T>
T ToT(const std::string & /* unused */, const std::string &key)
{
    return ToT<T>(key);
}

template <typename T>
std::string ToString(const T &val)
{
    if constexpr (std::is_same_v<std::remove_cv_t<T>, std::string>) {
        return val;
    } else {
        return std::to_string(val);
    }
}

/// The concrete config class should inherit the config base and define all the
/// entries.
class ConfigBase {
public:
    template <typename T>
    struct Entry {
        Entry(const std::string &_key, const T &_val,
            std::function<std::string(const T &)> _toStr = [](const T &val) { return ToString(val); },
            std::function<T(const std::string &, const std::string &)> _toT = [
                ](const std::string &k, const std::string &v) {
                return ToT<T>(v);
            }): key{_key}, defaultVal{_val}, toStr{_toStr}, toT{_toT} {}

        const std::string key;
        const T defaultVal;
        const std::function<std::string(const T &)> toStr;
        const std::function<T(const std::string &, const std::string &)> toT;
    };

    ConfigBase(std::unordered_map<std::string, std::string> &&configs, bool _mutable = false)
        : configs_(std::move(configs)), mutable_(_mutable) {}

    virtual ~ConfigBase() = default;

    template <typename T>
    ConfigBase &Set(const Entry<T> &entry, const T &val)
    {
        OMNI_CHECK(mutable_, "Cannot set in immutable config");
        std::unique_lock<std::shared_mutex> l(mutex_);
        configs_[entry.key] = entry.toStr(val);
        return *this;
    }

    ConfigBase &Set(const std::string &key, const std::string &val)
    {
        OMNI_CHECK(mutable_, "Cannot set in immutable config");
        std::unique_lock<std::shared_mutex> l(mutex_);
        configs_[key] = val;
        return *this;
    }

    template <typename T>
    ConfigBase &Unset(const Entry<T> &entry)
    {
        OMNI_CHECK(mutable_, "Cannot unset in immutable config");
        std::unique_lock<std::shared_mutex> l(mutex_);
        configs_.erase(entry.key);
        return *this;
    }

    ConfigBase &Reset()
    {
        OMNI_CHECK(mutable_, "Cannot reset in immutable config");
        std::unique_lock<std::shared_mutex> l(mutex_);
        configs_.clear();
        return *this;
    }

    template <typename T>
    T Get(const Entry<T> &entry) const
    {
        std::shared_lock<std::shared_mutex> l(mutex_);
        auto iter = configs_.find(entry.key);
        return iter != configs_.end() ? entry.toT(entry.key, iter->second) : entry.defaultVal;
    }

    template <typename T>
    std::optional<T> Get(const std::string &key) const
    {
        auto val = Get(key);
        if (val.has_value()) {
            return ToT<T>(val.value());
        } else {
            return std::nullopt;
        }
    }

    template <typename T>
    T Get(const std::string &key, const T &defaultValue) const
    {
        auto val = Get(key);
        if (val.has_value()) {
            return ToT<T>(key, val.value());
        } else {
            return defaultValue;
        }
    }

    bool ValueExists(const std::string &key) const
    {
        std::shared_lock<std::shared_mutex> l(mutex_);
        return configs_.find(key) != configs_.end();
    }

    const std::unordered_map<std::string, std::string> &RawConfigs() const;

    std::unordered_map<std::string, std::string> RawConfigsCopy() const;

protected:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> configs_;

private:
    std::optional<std::string> Get(const std::string &key) const
    {
        std::optional<std::string> val;
        std::shared_lock<std::shared_mutex> l(mutex_);
        auto it = configs_.find(key);
        if (it != configs_.end()) {
            val = it->second;
        }
        return val;
    }

    const bool mutable_;
};
}
