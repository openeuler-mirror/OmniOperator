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
public class Date32VecType extends VecType {
    /**
     * Date32 singleton
     */
    public static final Date32VecType DATE32 = new Date32VecType(DateUnit.DAY);

    @JsonProperty
    private final DateUnit dateUnit;

    /**
     * Date32 construct.
     *
     * @param dateUnit the unit of date
     */
    public Date32VecType(@JsonProperty("dateUnit") DateUnit dateUnit) {
        super(VecTypeId.OMNI_VEC_TYPE_DATE32);
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
