//
// Created by root on 2/6/26.
//
#include "QueryConfig.h"
#include "fmt/format.h"
#include "type/tz/TimeZoneMap.h"

namespace omniruntime::config {
void QueryConfig::ValidateConfig()
{
    // Validate if timezone name can be recognized.
    if (config_->ValueExists(QueryConfig::kSessionTimezone)) {
        OMNI_CHECK(tz::getTimeZoneID( config_->Get<std::string>(QueryConfig::kSessionTimezone).value(), false) != -1,
            fmt::format( "session '{}' set with invalid value '{}'", QueryConfig::kSessionTimezone, config_->Get<std::
                string>(QueryConfig::kSessionTimezone).value()));
    }
}
}
