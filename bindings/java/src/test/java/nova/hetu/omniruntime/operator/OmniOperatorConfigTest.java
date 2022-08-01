/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.operator.config.OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.createVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.config.SparkSpillConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Iterator;

/**
 * The omni operator config test.
 *
 * @since 2022-04-16
 */
public class OmniOperatorConfigTest {
    @Test
    public void TestSerialization() {
        final String spillPath = Paths.get("").toAbsolutePath().toString();

        // disable jit
        String noneConfigString = OperatorConfig.serialize(OperatorConfig.NONE);
        assertEquals(OperatorConfig.NONE, OperatorConfig.deserialize(noneConfigString));

        OperatorConfig invalidSpillConfig = new OperatorConfig(SpillConfig.INVALID);
        String invalidConfigString = OperatorConfig.serialize(invalidSpillConfig);
        assertEquals(invalidSpillConfig, OperatorConfig.deserialize(invalidConfigString));

        OperatorConfig sparkOperatorConfig1 = new OperatorConfig(new SparkSpillConfig(spillPath, 5));
        String sparkConfigString1 = OperatorConfig.serialize(sparkOperatorConfig1);
        assertEquals(sparkOperatorConfig1, OperatorConfig.deserialize(sparkConfigString1));

        OperatorConfig sparkOperatorConfig2 = new OperatorConfig(new SparkSpillConfig(false, spillPath, 1024, 1));
        String sparkConfigString2 = OperatorConfig.serialize(sparkOperatorConfig2);
        assertEquals(sparkOperatorConfig2, OperatorConfig.deserialize(sparkConfigString2));
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*OVERFLOW.*")
    public void TestOverflowConfig() {
        String exprJson = "{\"exprType\":\"FUNCTION\",\"returnType\":3,\"function_name\":\"overflow\","
                + "\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}]}";
        String[] exprs = {exprJson};
        DataType[] types = {IntDataType.INTEGER};
        Object[][] datas = {{0, 1, 2, 3}};

        OmniProjectOperatorFactory factoryNull = new OmniProjectOperatorFactory(exprs, types, 1,
                new OperatorConfig(new OverflowConfig(OVERFLOW_CONFIG_NULL)));
        VecBatch vecBatch1 = createVecBatch(types, datas);
        OmniOperator op1 = factoryNull.createOperator();
        op1.addInput(vecBatch1);

        Iterator<VecBatch> results1 = op1.getOutput();
        VecBatch resultVecBatch1 = results1.next();

        Object[][] expectDatas = {{null, null, null, null}};
        assertVecBatchEquals(resultVecBatch1, expectDatas);

        freeVecBatch(resultVecBatch1);
        op1.close();
        factoryNull.close();

        OmniProjectOperatorFactory factoryException = new OmniProjectOperatorFactory(exprs, types, 1,
                new OperatorConfig());

        VecBatch vecBatch2 = createVecBatch(types, datas);
        OmniOperator op2 = factoryException.createOperator();
        op2.addInput(vecBatch2);

        op2.close();
        factoryException.close();
    }
}
