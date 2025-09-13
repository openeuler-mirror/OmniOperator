/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.SparkSpillConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;

import org.testng.annotations.Test;

import java.nio.file.Paths;

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
}
