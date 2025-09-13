/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * date32 data type.
 *
 * @since 2021-08-05
 */
public class Date32DataType extends DataType {
    /**
     * Date32 singleton.
     */
    public static final Date32DataType DATE32 = new Date32DataType(DateUnit.DAY);

    private static final long serialVersionUID = 8120887624931817382L;

    @JsonProperty
    private final DateUnit dateUnit;

    /**
     * Date32 construct.
     *
     * @param dateUnit the unit of date
     */
    public Date32DataType(@JsonProperty("dateUnit") DateUnit dateUnit) {
        super(DataTypeId.OMNI_DATE32);
        this.dateUnit = dateUnit;
    }

    public DateUnit getDateUnit() {
        return dateUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateUnit, super.getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Date32DataType other = (Date32DataType) obj;
        return (Objects.equals(dateUnit, other.getDateUnit()) && Objects.equals(super.getId(), other.getId()));
    }
}
