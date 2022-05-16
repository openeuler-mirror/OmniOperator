/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;
import nova.hetu.omniruntime.type.Decimal128DataType;

import org.testng.annotations.Test;

import java.util.List;

/**
 * OmniExprVerify test.
 *
 * @since 2022-05-16
 */
public class OmniExprVerifyTest {
    /**
     * Test for Spark.
     */
    @Test
    public void exprVerifierForSpark() {
        DataType[] inputTypes = {new Decimal128DataType(21, 5)};
        List<String> projectionsJSON = ImmutableList
                .of("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":7,\"precision\":21,\"scale\":5,\"colVal\":0}");
        String filterJSON = "{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\","
                + "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":7,\"colVal\":0,"
                + "\"precision\":21,\"scale\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":7,"
                + "\"precision\":21,\"scale\":5,\"isNull\":false,\"value\":\"2000\"}}";

        long isSupported = new OmniExprVerify().exprVerifyNative(DataTypeSerializer.serialize(inputTypes), 0,
                filterJSON, projectionsJSON.toArray(new Object[0]), projectionsJSON.size(), 1);

        assertEquals(isSupported, 0);
    }
}
