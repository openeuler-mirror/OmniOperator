//
// Created by root on 10/10/25.
//

#include "IsNull.h"
#include "vector/vector.h"
#include "../VectorFunction.h"

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
        for (int i = 0; i < size; i++) {
            raw[i] = false;
        }
    }
};
}

void RegisterIsNullFunction(const std::string &name)
{
    std::vector<DataTypeId> param = {OMNI_INT};
    auto signature = std::make_shared<codegen::FunctionSignature>(name, param, OMNI_BOOLEAN);
    VectorFunction::functionMap_.insert(std::make_pair(signature, std::make_shared<IsNullFunction>()));
}
}
