/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: loser tree implementation
 */

#ifndef OMNI_RUNTIME_LOSERTREE_H
#define OMNI_RUNTIME_LOSERTREE_H

namespace omniruntime {
namespace op {
template <typename MergeStream, typename StreamIdx = uint16_t> class LoserTree {
public:
    LoserTree(const std::vector<MergeStream *> &streams) : streams(streams)
    {
        int32_t mergeStreamSize = static_cast<int32_t>(streams.size());
        int32_t sizePerLevel = 1;
        int32_t size = 0;
        while (mergeStreamSize > sizePerLevel) {
            size += sizePerLevel;
            sizePerLevel = sizePerLevel * 2;
        }

        auto nextPowerOfTwo = static_cast<int32_t>(NextPowerOfTwo(mergeStreamSize));
        if (mergeStreamSize == nextPowerOfTwo) {
            firstStream = size;
        } else {
            auto secondLastSize = sizePerLevel / 2;
            auto overflow = mergeStreamSize - secondLastSize;
            firstStream = (size - secondLastSize) + overflow;
        }
        values.resize(firstStream, EMPTY);
        equals.resize(firstStream, 0);
    }

    ~LoserTree()
    {
        for (auto stream : streams) {
            delete stream;
        }
        streams.clear();
    }

    ALWAYS_INLINE MergeStream *Next()
    {
        if (lastIdx == EMPTY) {
            if (values.empty()) {
                return streams[0]->HasData() ? streams[0] : nullptr;
            }
            lastIdx = First(0);
        } else {
            auto value = streams[lastIdx]->HasData() ? lastIdx : EMPTY;
            lastIdx = Propagate(Parent(firstStream + lastIdx), value);
        }
        return lastIdx == EMPTY ? nullptr : streams[lastIdx];
    }

    ALWAYS_INLINE MergeStream *NextWithEqual(bool &isEqual)
    {
        if (lastIdx == EMPTY) {
            if (values.empty()) {
                isEqual = false;
                return streams[0]->HasData() ? streams[0] : nullptr;
            }
            lastIdx = FirstWithEqual(0, isEqual);
        } else {
            auto value = streams[lastIdx]->HasData() ? lastIdx : EMPTY;
            lastIdx = PropagateWithEqual(Parent(firstStream + lastIdx), value, isEqual);
        }
        if (lastIdx == EMPTY) {
            isEqual = false;
            return nullptr;
        } else {
            return streams[lastIdx];
        }
    }

private:
    static constexpr StreamIdx EMPTY = std::numeric_limits<StreamIdx>::max();

    ALWAYS_INLINE uint64_t NextPowerOfTwo(uint64_t size)
    {
        if (size == 0) {
            return 0;
        }
        uint32_t bits = 63 - __builtin_clzll(size);
        uint64_t lower = 1ULL << bits;
        if (lower == size) {
            return size;
        }
        return 2 * lower;
    }

    ALWAYS_INLINE StreamIdx Parent(StreamIdx node)
    {
        return (node - 1) / 2;
    }

    ALWAYS_INLINE StreamIdx LeftChild(StreamIdx node)
    {
        return node * 2 + 1;
    }

    ALWAYS_INLINE StreamIdx RightChild(StreamIdx node)
    {
        return node * 2 + 2;
    }

    StreamIdx First(StreamIdx node)
    {
        if (node >= firstStream) {
            auto streamIdx = node - firstStream;
            return streams[streamIdx]->HasData() ? streamIdx : EMPTY;
        }

        auto leftNode = First(LeftChild(node));
        auto rightNode = First(RightChild(node));
        if (leftNode == EMPTY) {
            return rightNode;
        } else if (rightNode == EMPTY) {
            return leftNode;
        } else if (*(streams[leftNode]) < *(streams[rightNode])) {
            values[node] = rightNode;
            return leftNode;
        } else {
            values[node] = leftNode;
            return rightNode;
        }
    }

    ALWAYS_INLINE StreamIdx Propagate(StreamIdx node, StreamIdx value)
    {
        while (values[node] == EMPTY) {
            if (node == 0) {
                return value;
            }
            node = Parent(node);
        }

        for (;;) {
            if (values[node] == EMPTY) {
                // do nothing
            } else if (value == EMPTY) {
                value = values[node];
                values[node] = EMPTY;
            } else if (*(streams[values[node]]) < *(streams[value])) {
                std::swap(value, values[node]);
            } else {
                // do nothing
            }

            if (node == 0) {
                return value;
            }
            node = Parent(node);
        }
    }

    StreamIdx FirstWithEqual(StreamIdx node, bool &isEqual)
    {
        if (node >= firstStream) {
            isEqual = false;
            auto streamIdx = node - firstStream;
            return streams[streamIdx]->HasData() ? streamIdx : EMPTY;
        }

        bool isLeftEqual = false;
        bool isRightEqual = false;
        auto leftNode = FirstWithEqual(LeftChild(node), isLeftEqual);
        auto rightNode = FirstWithEqual(RightChild(node), isRightEqual);
        if (leftNode == EMPTY) {
            isEqual = isRightEqual;
            return rightNode;
        } else if (rightNode == EMPTY) {
            isEqual = isLeftEqual;
            return leftNode;
        } else {
            auto result = streams[leftNode]->CompareTo(*(streams[rightNode]));
            if (result == 0) {
                values[node] = rightNode;
                equals[node] = isRightEqual;
                isEqual = true;
                return leftNode;
            } else if (result < 0) {
                values[node] = rightNode;
                equals[node] = isRightEqual;
                isEqual = isLeftEqual;
                return leftNode;
            } else {
                values[node] = leftNode;
                equals[node] = isLeftEqual;
                isEqual = isRightEqual;
                return rightNode;
            }
        }
    }

    ALWAYS_INLINE StreamIdx PropagateWithEqual(StreamIdx node, StreamIdx value, bool &isEqual)
    {
        isEqual = false;
        while (values[node] == EMPTY) {
            if (node == 0) {
                return value;
            }
            node = Parent(node);
        }

        for (;;) {
            if (values[node] == EMPTY) {
                // do nothing
            } else if (value == EMPTY) {
                value = values[node];
                isEqual = equals[node];
                values[node] = EMPTY;
                equals[node] = false;
            } else {
                auto result = streams[values[node]]->CompareTo(*(streams[value]));
                if (result == 0) {
                    isEqual = true;
                } else if (result < 0) {
                    auto tmpNode = values[node];
                    auto tmpIsEqual = equals[node];
                    values[node] = value;
                    equals[node] = isEqual;
                    value = tmpNode;
                    isEqual = tmpIsEqual;
                } else {
                    // do nothing
                }
            }

            if (node == 0) {
                return value;
            }
            node = Parent(node);
        }
    }

    std::vector<MergeStream *> streams;
    int32_t firstStream;
    std::vector<StreamIdx> values;
    std::vector<uint8_t> equals;
    StreamIdx lastIdx = EMPTY;
};
}
}

#endif // OMNI_RUNTIME_LOSERTREE_H
