/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * date32 vec type
 *
 * @since 2021-08-05
 */
public class Date64VecType extends VecType {
    /**
     * Date64 singleton
     */
    public static final Date64VecType DATE64 = new Date64VecType(DateUnit.DAY);

    @JsonProperty
    private final VecType.DateUnit dateUnit;

    /**
     * date 64 construct
     *
     * @param dateUnit the unit of date
     */
    public Date64VecType(@JsonProperty("dateUnit") VecType.DateUnit dateUnit) {
        super(VecTypeId.OMNI_VEC_TYPE_DATE64);
        this.dateUnit = dateUnit;
    }

    public DateUnit getDateUnit() {
        return dateUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateUnit, super.getId());
    }
}
