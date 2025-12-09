//
// Created by root on 10/10/25.
//

#include "IsNull.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;

namespace {
class IsNullFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        const auto arg = args.top();
        args.pop();
        const auto raw = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<bool> *>(result));
        const auto size = arg->GetSize();

        memset(raw, 0, size * sizeof(bool));

        const auto nullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(arg));
        SelectivityVector rows(size);
        rows.setFromBits(nullBits, size);
        rows.applyToSelected([&](vector_size_t i) { raw[i] = true; });
    }
};
}

void RegisterIsNullFunction(const std::string &name)
{
    auto signatures = IsNullSignatures(name);
    for (const auto &signature : signatures) {
        VectorFunction::functionMap_.insert(std::make_pair(signature, std::make_shared<IsNullFunction>()));
    }
}
}
