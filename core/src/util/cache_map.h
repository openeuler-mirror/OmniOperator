/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: FilterAndProject operator header
 */

#pragma once
#include <iostream>
#include <unordered_map>
#include <list>
#include <optional>

template <typename KeyType, typename ValueType>
class CacheMap {
public:
    explicit CacheMap(size_t capacity) : capacity_(capacity) {}

    std::optional<ValueType> Get(const KeyType &url)
    {
        auto it = map_.find(url);
        if (it == map_.end()) {
            return std::nullopt;
        }
        cacheList_.splice(cacheList_.begin(), cacheList_, it->second.second);

        return it->second.first;
    }

    void SetCount(const KeyType &url)
    {
        auto it = map_.find(url);
        if (it == map_.end()) {
            return;
        }
        cacheList_.splice(cacheList_.begin(), cacheList_, it->second.second);
    }

    template <typename V, std::enable_if_t<std::is_same_v<std::remove_reference_t<std::remove_cv_t<V>>, ValueType>>* =
        nullptr>
    void Set(const KeyType &key, V &&value)
    {
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.first = std::forward<V>(value);
            cacheList_.splice(cacheList_.begin(), cacheList_, it->second.second);
            return;
        }

        if (map_.size() >= capacity_) {
            auto keyIterator = cacheList_.back();
            map_.erase(keyIterator);
            cacheList_.pop_back();
        }

        cacheList_.push_front(key);
        map_[key] = std::pair(std::forward<V>(value), cacheList_.begin());
    }

    void Clear()
    {
        map_.clear();
        cacheList_.clear();
    }

private:
    size_t capacity_;
    std::list<KeyType> cacheList_;
    std::unordered_map<KeyType, std::pair<ValueType, typename std::list<KeyType>::iterator>> map_;
};