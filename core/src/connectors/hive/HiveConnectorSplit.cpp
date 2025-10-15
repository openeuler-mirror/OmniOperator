/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <cstring>

#include "HiveConnectorSplit.h"
#include "TableHandle.h"
#include "type/data_type_serializer.h"

namespace omniruntime::connector::hive {

std::string HiveConnectorSplit::getFileName() const
{
    const auto i = filePath.rfind('/');
    return i == std::string::npos ? filePath : filePath.substr(i + 1);
}

codegen::FileFormat HiveConnectorSplit::getFileFormat() const
{
    return fileFormat;
}

std::string HiveConnectorSplit::getFilePath() const
{
    return filePath;
}

} // namespace
