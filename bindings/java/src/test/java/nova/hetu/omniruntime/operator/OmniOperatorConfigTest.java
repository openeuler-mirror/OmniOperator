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

        // enable jit
        OperatorConfig noneConfigWithJit = new OperatorConfig(true, SpillConfig.NONE);
        String noneConfigWithJitString = OperatorConfig.serialize(noneConfigWithJit);
        assertEquals(noneConfigWithJit, OperatorConfig.deserialize(noneConfigWithJitString));

        OperatorConfig invalidConfigWithJit = new OperatorConfig(true, SpillConfig.INVALID);
        String invalidConfigWithJitString = OperatorConfig.serialize(invalidConfigWithJit);
        assertEquals(invalidConfigWithJit, OperatorConfig.deserialize(invalidConfigWithJitString));

        OperatorConfig sparkConfigWithJit1 = new OperatorConfig(true, new SparkSpillConfig(spillPath, 5));
        String sparkConfigWithJitString1 = OperatorConfig.serialize(sparkConfigWithJit1);
        assertEquals(sparkConfigWithJit1, OperatorConfig.deserialize(sparkConfigWithJitString1));

        OperatorConfig sparkConfigWithJit2 = new OperatorConfig(true, new SparkSpillConfig(false, spillPath, 1024, 1));
        String sparkConfigWithJitString2 = OperatorConfig.serialize(sparkConfigWithJit2);
        assertEquals(sparkConfigWithJit2, OperatorConfig.deserialize(sparkConfigWithJitString2));
    }
}
