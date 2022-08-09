/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.type;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * test vec type
 *
 * @since 2021-7-2
 */
public class TestDataType {
    /**
     * test vec type
     */
    @Test
    public void testDataType() {
        DataType type = getDataTypeFromBase("BIGINT");
        assertEquals(type, LongDataType.LONG);
    }

    private DataType getDataTypeFromBase(String base) {
        switch (base) {
            case "INT":
            case "DATE":
                return IntDataType.INTEGER;
            case "BIGINT":
                return LongDataType.LONG;
            case "DOUBLE":
                return DoubleDataType.DOUBLE;
            case "BOOLEAN":
                return BooleanDataType.BOOLEAN;
            default:
                throw new OmniRuntimeException(OmniErrorType.OMNI_UNDEFINED, "Not support Type " + base);
        }
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        ObjectMapper map = new ObjectMapper();
        List<nova.hetu.omniruntime.type.DataType> types = new ArrayList<>();
        types.add(LongDataType.LONG);
        types.add(new Decimal128DataType(1, 2));
        assertEquals(map.writeValueAsString(types), "[{\"id\":2},{\"precision\":1,\"scale\":2,\"id\":7}]");
    }
}
