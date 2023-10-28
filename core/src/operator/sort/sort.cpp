/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort implementations
 */
#include "sort.h"
#include "util/type_util.h"
#include "util/debug.h"
#include "vector/vector_helper.h"
#include "operator/util/operator_util.h"
#include "operator/spill/vector_batch_spiller.h"
#include "operator/spill/spill_iterator.h"
#include "util/omni_exception.h"
#include <dirent.h>
#include <sys/stat.h>

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortOperatorFactory::SortOperatorFactory(const DataTypes &dataTypes, int32_t *outputCols, int32_t outputColCount,
    int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    const OperatorConfig &operatorConfig)
    : sourceTypes(dataTypes), operatorConfig(operatorConfig)
{
    this->outputCols.insert(this->outputCols.end(), outputCols, outputCols + outputColCount);
    this->sortCols.insert(this->sortCols.end(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.end(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.end(), sortNullFirsts, sortNullFirsts + sortColCount);
}

SortOperatorFactory::~SortOperatorFactory() = default;

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(const DataTypes &dataTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    OperatorConfig defaultConfig;
    return CreateSortOperatorFactory(dataTypes, outputCols, outputColCount, sortCols, sortAscendings, sortNullFirsts,
        sortColCount, defaultConfig);
}

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(const DataTypes &dataTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    const OperatorConfig &operatorConfig)
{
    OperatorConfig::CheckOperatorConfig(operatorConfig);
    auto pOperatorFactory = new SortOperatorFactory(dataTypes, outputCols, outputColCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, operatorConfig);
    return pOperatorFactory;
}

Operator *SortOperatorFactory::CreateOperator()
{
    auto pSortOperator =
        new SortOperator(sourceTypes, outputCols, sortCols, sortAscendings, sortNullFirsts, operatorConfig);
    return pSortOperator;
}

// function implements for class Sort
SortOperator::SortOperator(const DataTypes &dataTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
    std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts, const OperatorConfig &operatorConfig)
    : sourceTypes(dataTypes), operatorConfig(operatorConfig)
{
    this->outputCols = outputCols;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->pagesIndex = std::make_unique<PagesIndex>(sourceTypes);
    maxRowCountPerBatch = OperatorUtil::GetMaxRowCount(dataTypes.Get(), outputCols.data(), outputCols.size());
    maxRowCountPerBatch = maxRowCountPerBatch == 0 ? 1 : maxRowCountPerBatch;
    if (sourceTypes.GetSize() == 1 && sourceTypes.GetType(0)->GetId() != OMNI_VARCHAR &&
        sourceTypes.GetType(0)->GetId() != OMNI_CHAR && sourceTypes.GetType(0)->GetId() != OMNI_BOOLEAN) {
        canInplaceSort = true;
    }

    const char *dir = "/opt/sort/";
    struct stat info;
    if (stat(dir, &info) != 0) {
        mkdir(dir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    std::stringstream fileStream;
    fileStream << std::string(dir);
    auto tid = std::this_thread::get_id();
    std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds >(
           std::chrono::system_clock::now().time_since_epoch()
    );
    fileStream << tid;
    fileStream << "_";
    fileStream << ms.count();
    fileStream << ".txt";
    std::string fileName = fileStream.str();
    file.open(fileName, std::ios::app);
    file << "sortCols={";
    for (auto sortCol : sortCols) {
        file << sortCol << " ";
    }
    file << "}" << std::endl;
}

SortOperator::~SortOperator() = default;

template <type::DataTypeId typeId> static void PrintFlatVectorValue(BaseVector *vector, int32_t rowIndex, std::ofstream &file)
{
    using namespace omniruntime::type;
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view>) {
        file << std::dec << static_cast<Vector<LargeStringContainer<T>> *>(vector)->GetValue(rowIndex) << "\t";
    } else {
        file << std::dec << static_cast<Vector<T> *>(vector)->GetValue(rowIndex) << "\t";
    }
}

template <type::DataTypeId typeId> static void PrintDictionaryVectorValue(BaseVector *vector, int32_t rowIndex, std::ofstream &file)
{
    using namespace omniruntime::type;
    using T = typename NativeType<typeId>::type;
    using DictionaryVarchar = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
    if constexpr (std::is_same_v<T, std::string_view>) {
        file << std::dec << static_cast<DictionaryVarchar *>(vector)->GetValue(rowIndex) << "\t";
    } else {
        file << std::dec << static_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(rowIndex) << "\t";
    }
}

static void PrintVectorValue(BaseVector *vector, int32_t rowIndex, std::ofstream &file)
{
    using namespace omniruntime::type;
    if (vector->IsNull(rowIndex)) {
        file << "NULL" << "\t";
        return;
    }

    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        DYNAMIC_TYPE_DISPATCH(PrintFlatVectorValue, vector->GetTypeId(), vector, rowIndex, file);
    } else {
        DYNAMIC_TYPE_DISPATCH(PrintDictionaryVectorValue, vector->GetTypeId(), vector, rowIndex, file);
    }
}

void PrintVecBatchToFile(VectorBatch *vecBatch, std::ofstream &file)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    int32_t rowCount = vecBatch->GetRowCount();
    for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
        for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
            auto vector = vecBatch->Get(colIdx);
            PrintVectorValue(vector, rowIdx, file);
        }
        file << std::endl;
    }
}

static void PrintType(DataTypeId typeId, std::ofstream &file)
{
    switch (typeId) {
        case type::OMNI_INT:
            file << "Int,";
            break;
        case type::OMNI_SHORT:
            file << "Short,";
            break;
        case type::OMNI_LONG:
            file << "Long,";
            break;
        case type::OMNI_DECIMAL64:
            file << "ShortDecimal,";
            break;
        case type::OMNI_DECIMAL128:
            file << "LongDecimal,";
            break;
        case type::OMNI_DOUBLE:
            file << "Double,";
            break;
        case type::OMNI_DATE32:
            file << "Date32,";
            break;
        case type::OMNI_CHAR:
            file << "Char,";
            break;
        case type::OMNI_VARCHAR:
            file << "Varchar,";
            break;
        case type::OMNI_BOOLEAN:
            file << "Bool,";
            break;
        default:
            file << "Other,";
            break;
    }
}

int32_t SortOperator::AddInput(VectorBatch *vecBatch)
{
    if (first) {
        // print input types and output types
        file << "inputTypes=";
        auto vecCount = vecBatch->GetVectorCount();
        for (int i = 0; i < vecCount; i++) {
            auto vector = vecBatch->Get(i);
            auto typeId = vector->GetTypeId();
            PrintType(typeId, file);
        }
        file << std::endl;
        first = false;
    }
    file << "SortOperator::AddInput rowCount=" << vecBatch->GetRowCount() << std::endl;
    PrintVecBatchToFile(vecBatch, file);

    totalRowCount += vecBatch->GetRowCount();
    pagesIndex->AddVecBatch(vecBatch);
    if (operatorConfig.GetSpillConfig()->NeedSpill(pagesIndex.get())) {
        auto result = SpillToDisk();
        pagesIndex->Clear();
        if (result != ErrorCode::SUCCESS) {
            throw omniruntime::exception::OmniException(GetErrorCode(result), GetErrorMessage(result));
        }
    }
    return 0;
}

// return error code
int32_t SortOperator::GetOutput(VectorBatch **outputVecBatch)
{
    // input data is empty, or all data has been returned.
    if (totalRowCount == 0 || totalRowCount == rowCountOutputted) {
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        file.close();
        return 0;
    }
    if (spiller == nullptr) {
        GetOutputFromMemory(outputVecBatch);
    } else {
        MergeFromDiskAndMemory(outputVecBatch);
    }
    if ((*outputVecBatch)->GetRowCount() != 0) {
        auto output = *outputVecBatch;
        file << "SortOperator::GetOutput rowCount=" << output->GetRowCount() << std::endl;
        PrintVecBatchToFile(output, file);
    }

    // through the reference counting mechanism, can vecBatch be released early?
    if (totalRowCount == rowCountOutputted) { // all result have been generated
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        file.close();
        return 0;
    }
    return 0;
}

OmniStatus SortOperator::Close()
{
    if (comparator) {
        delete comparator;
    }
    if (spiller) {
        delete spiller;
    }
    // ensure free pagesIndex if exception occurs
    pagesIndex->Clear();
    return OMNI_STATUS_NORMAL;
}

ErrorCode SortOperator::SpillToDisk()
{
    if (!canInplaceSort) {
        pagesIndex->Prepare();
    } else {
        DYNAMIC_TYPE_DISPATCH(pagesIndex->PrepareInplaceSort, sourceTypes.GetType(0)->GetId(), sortNullFirsts[0]);
    }

    Sort();

    if (spiller == nullptr) {
        comparator = new VecBatchWithPositionComparator(sourceTypes, sortCols, sortAscendings, sortNullFirsts);
        spiller = new VectorBatchSpiller(operatorConfig.GetSpillConfig()->GetSpillPath(), sourceTypes, outputCols,
            comparator);
        spiller->SetSpillTracker(GetRootSpillTracker().CreateSpillTracker());
    }

    // spill data from memory to disk
    std::vector<VectorBatch *> vecBatchesForSpill;
    GetVecBatchesForSpill(vecBatchesForSpill);

    VectorBatchUnitIter iter(vecBatchesForSpill);
    auto result = spiller->Spill(iter);
    VectorHelper::FreeVecBatches(vecBatchesForSpill);
    return result;
}

void SortOperator::Sort()
{
    int32_t positionCount = pagesIndex->GetRowCount();
    int32_t sortColCount = sortCols.size();
    if (!canInplaceSort) {
        pagesIndex->Sort(sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, 0, positionCount);
    } else {
        pagesIndex->SortInplace(sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, 0,
            positionCount);
    }
}

void SortOperator::GetVecBatchesForSpill(std::vector<VectorBatch *> &vecBatchesForSpill)
{
    int32_t typesCount = sourceTypes.GetSize();
    std::vector<int32_t> outputCols(typesCount);
    for (int32_t i = 0; i < typesCount; i++) {
        outputCols[i] = i;
    }

    pagesIndex->GetSortedVecBatches(outputCols, vecBatchesForSpill, canInplaceSort);
}

void SortOperator::PrepareOutput()
{
    if (pagesIndex->GetRowCount() <= 0 || hasSorted) {
        return;
    }
    if (!canInplaceSort) {
        pagesIndex->Prepare();
    } else {
        DYNAMIC_TYPE_DISPATCH(pagesIndex->PrepareInplaceSort, sourceTypes.GetType(0)->GetId(), sortNullFirsts[0]);
    }
    // first step, sort
    Sort();
    hasSorted = true;
}

void SortOperator::GetOutputFromMemory(VectorBatch **outputVecBatch)
{
    // first if has not sorted,need to sort first.
    PrepareOutput();
    // second step, get sorted vector batches
    int32_t rowCountToOutput =
        static_cast<int32_t>(std::min(static_cast<size_t>(maxRowCountPerBatch), (totalRowCount - rowCountOutputted)));

    auto *result = new VectorBatch(rowCountToOutput);
    if (!canInplaceSort) {
        pagesIndex->GetOutput(outputCols.data(), outputCols.size(), result, sourceTypes.GetIds(), rowCountOutputted,
            rowCountToOutput);
    } else {
        pagesIndex->GetOutputInplaceSort(outputCols.data(), outputCols.size(), result, sourceTypes.GetIds(),
            rowCountOutputted, rowCountToOutput);
    }
    rowCountOutputted += rowCountToOutput;
    *outputVecBatch = result;
}

void SortOperator::MergeFromDiskAndMemory(VectorBatch **outputVecBatch)
{
    if (!hasSorted) {
        std::vector<VectorBatch *> vecBatchesForSpill;
        if (pagesIndex->GetRowCount() > 0) {
            if (!canInplaceSort) {
                pagesIndex->Prepare();
            } else {
                DYNAMIC_TYPE_DISPATCH(pagesIndex->PrepareInplaceSort, sourceTypes.GetType(0)->GetId(),
                    sortNullFirsts[0]);
            }
            // first step, sort
            Sort();
            // second step, get sorted vector batches
            GetVecBatchesForSpill(vecBatchesForSpill);
        }

        // third step, merge data from disk and memory
        VectorBatchUnitIter memoryIter(vecBatchesForSpill);
        spiller->MergeFromDiskAndMemory(memoryIter);
        hasSorted = true;
        hasNext = spiller->HasNext();
    }
    if (hasNext) {
        auto *vectorBatchUnit = static_cast<VectorBatchUnit *>(spiller->Next());
        auto *result = vectorBatchUnit->GetVectorBatch();
        *outputVecBatch = result;
        rowCountOutputted += result->GetRowCount();
        delete vectorBatchUnit;
    }
    hasNext = spiller->HasNext();
}
} // end of namespace op
} // end of namespace omniruntime