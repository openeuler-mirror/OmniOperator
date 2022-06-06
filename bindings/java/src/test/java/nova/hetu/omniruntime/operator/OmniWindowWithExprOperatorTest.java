/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package nova.hetu.omniruntime.operator;

import static nova.hetu.omniruntime.constants.OmniWindowFrameBoundType.OMNI_FRAME_BOUND_CURRENT_ROW;
import static nova.hetu.omniruntime.constants.OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING;
import static nova.hetu.omniruntime.constants.OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING;
import static nova.hetu.omniruntime.constants.OmniWindowFrameType.OMNI_FRAME_TYPE_RANGE;
import static nova.hetu.omniruntime.constants.OmniWindowFrameType.OMNI_FRAME_TYPE_ROWS;
import static nova.hetu.omniruntime.util.TestUtils.assertVecBatchEquals;
import static nova.hetu.omniruntime.util.TestUtils.freeVecBatch;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonFieldReference;
import static nova.hetu.omniruntime.util.TestUtils.getOmniJsonLiteral;
import static nova.hetu.omniruntime.util.TestUtils.omniFunctionExpr;
import static nova.hetu.omniruntime.util.TestUtils.omniJsonFourArithmeticExpr;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.constants.OmniWindowFrameBoundType;
import nova.hetu.omniruntime.constants.OmniWindowFrameType;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.window.OmniWindowWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The type Omni window operator test.
 *
 * @since 2021-11-3
 */
public class OmniWindowWithExprOperatorTest {
    /**
     * Test max.
     */
    @Test
    public void testMax() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        int[] outputChannels = {0, 1, 2};
        FunctionType[] windowFunction = {FunctionType.OMNI_AGGREGATION_TYPE_MAX};
        OmniWindowFrameType[] windowFrameTypes = {OMNI_FRAME_TYPE_RANGE};
        OmniWindowFrameBoundType[] windowFrameStartTypes = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
        int[] winddowFrameStartChannels = {-1};
        OmniWindowFrameBoundType[] windowFrameEndTypes = {OMNI_FRAME_BOUND_CURRENT_ROW};
        int[] winddowFrameEndChannels = {-1};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {0};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        String[] argumentKeys = {omniJsonFourArithmeticExpr("ADD", 3, getOmniJsonFieldReference(3, 2),
                getOmniJsonLiteral(3, false, 50))};
        DataType[] windowFunctionReturnType = {DoubleDataType.DOUBLE};
        OmniWindowWithExprOperatorFactory omniWindowOperatorFactory = new OmniWindowWithExprOperatorFactory(sourceTypes,
                outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                sortNullFirsts, preSortedChannelPrefix, 10000, argumentKeys, windowFunctionReturnType, windowFrameTypes,
                windowFrameStartTypes, winddowFrameStartChannels, windowFrameEndTypes, winddowFrameEndChannels);
        OmniOperator omniOperator = omniWindowOperatorFactory.createOperator();

        VecBatch vecBatch = buildData();

        omniOperator.addInput(vecBatch);
        Iterator<VecBatch> output = omniOperator.getOutput();
        VecBatch outputVecBatch = output.next();
        Object[][] expectedDatas = {{0, 0, 1, 1, 2, 2}, {8L, 8L, 4L, 1L, 5L, 2L}, {6.6D, 3.3D, 2.2D, 5.5D, 1.1D, 4.4D},
                {56.6D, 53.3D, 52.2D, 55.5D, 51.1D, 54.4D}, {56.6D, 56.6D, 52.2D, 55.5D, 51.1D, 54.4D}};
        assertVecBatchEquals(outputVecBatch, expectedDatas);
        freeVecBatch(outputVecBatch);

        omniOperator.close();
        omniWindowOperatorFactory.close();
    }

    @Test(expectedExceptions = OmniRuntimeException.class, expectedExceptionsMessageRegExp = ".*EXPRESSION_NOT_SUPPORT.*")
    public void testWindowWithInvalidKeys() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        int[] outputChannels = {0, 1, 2};
        FunctionType[] windowFunction = {FunctionType.OMNI_AGGREGATION_TYPE_MAX};
        OmniWindowFrameType[] windowFrameTypes = {OMNI_FRAME_TYPE_RANGE};
        OmniWindowFrameBoundType[] windowFrameStartTypes = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
        int[] winddowFrameStartChannels = {-1};
        OmniWindowFrameBoundType[] windowFrameEndTypes = {OMNI_FRAME_BOUND_CURRENT_ROW};
        int[] winddowFrameEndChannels = {-1};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {0};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        String[] argumentKeys = {omniFunctionExpr("abc", 3, getOmniJsonFieldReference(3, 2))};
        DataType[] windowFunctionReturnType = {DoubleDataType.DOUBLE};
        OmniWindowWithExprOperatorFactory omniWindowOperatorFactory = new OmniWindowWithExprOperatorFactory(sourceTypes,
                outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                sortNullFirsts, preSortedChannelPrefix, 10000, argumentKeys, windowFunctionReturnType, windowFrameTypes,
                windowFrameStartTypes, winddowFrameStartChannels, windowFrameEndTypes, winddowFrameEndChannels);
    }

    @Test
    public void testFactoryJitContextEquals() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        int[] outputChannels = {0, 1, 2};
        FunctionType[] windowFunction = {FunctionType.OMNI_AGGREGATION_TYPE_MAX};
        OmniWindowFrameType[] windowFrameTypes = {OMNI_FRAME_TYPE_RANGE};
        OmniWindowFrameBoundType[] windowFrameStartTypes = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
        int[] winddowFrameStartChannels = {-1};
        OmniWindowFrameBoundType[] windowFrameEndTypes = {OMNI_FRAME_BOUND_CURRENT_ROW};
        int[] winddowFrameEndChannels = {-1};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {1};
        int[] sortOrder = {0};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        String[] argumentKeys = {omniJsonFourArithmeticExpr("ADD", 3, getOmniJsonFieldReference(3, 2),
                getOmniJsonLiteral(3, false, 50))};
        DataType[] windowFunctionReturnType = {DoubleDataType.DOUBLE};
        OmniWindowWithExprOperatorFactory.JitContext factory1 = new OmniWindowWithExprOperatorFactory.JitContext(
                sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels,
                sortOrder, sortNullFirsts, preSortedChannelPrefix, 10000, argumentKeys, windowFunctionReturnType,
                windowFrameTypes, windowFrameStartTypes, winddowFrameStartChannels, windowFrameEndTypes,
                winddowFrameEndChannels, new OperatorConfig());
        OmniWindowWithExprOperatorFactory.JitContext factory2 = new OmniWindowWithExprOperatorFactory.JitContext(
                sourceTypes, outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels,
                sortOrder, sortNullFirsts, preSortedChannelPrefix, 10000, argumentKeys, windowFunctionReturnType,
                windowFrameTypes, windowFrameStartTypes, winddowFrameStartChannels, windowFrameEndTypes,
                winddowFrameEndChannels, new OperatorConfig());
        OmniWindowWithExprOperatorFactory.JitContext factory3 = null;
        assertTrue(factory1.equals(factory2));
        assertTrue(factory1.equals(factory1));
        assertFalse(factory1.equals(factory3));
    }

    @Test
    public void testWindowFunctionMix() {
        DataType[] sourceTypes = {IntDataType.INTEGER, LongDataType.LONG, DoubleDataType.DOUBLE};
        int[] outputChannels = {0, 1, 2};
        FunctionType[] windowFunction = {FunctionType.OMNI_WINDOW_TYPE_RANK, FunctionType.OMNI_AGGREGATION_TYPE_AVG};
        OmniWindowFrameType[] windowFrameTypes = {OMNI_FRAME_TYPE_ROWS, OMNI_FRAME_TYPE_ROWS};
        OmniWindowFrameBoundType[] windowFrameStartTypes = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
        int[] winddowFrameStartChannels = {-1, -1};
        OmniWindowFrameBoundType[] windowFrameEndTypes = {OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING,
                OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING};
        int[] winddowFrameEndChannels = {-1, -1};
        int[] partitionChannels = {0};
        int[] preGroupedChannels = {};
        int[] sortChannels = {2};
        int[] sortOrder = {1};
        int[] sortNullFirsts = {0};
        int preSortedChannelPrefix = 0;
        String[] argumentKeys = {omniFunctionExpr("abs", 2, getOmniJsonFieldReference(2, 1))};
        DataType[] windowFunctionReturnType = {IntDataType.INTEGER, DoubleDataType.DOUBLE};
        OmniWindowWithExprOperatorFactory omniWindowOperatorFactory = new OmniWindowWithExprOperatorFactory(sourceTypes,
                outputChannels, windowFunction, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                sortNullFirsts, preSortedChannelPrefix, 10000, argumentKeys, windowFunctionReturnType, windowFrameTypes,
                windowFrameStartTypes, winddowFrameStartChannels, windowFrameEndTypes, winddowFrameEndChannels);
        OmniOperator omniOperator = omniWindowOperatorFactory.createOperator();

        VecBatch vecBatch = buildData();

        omniOperator.addInput(vecBatch);
        Iterator<VecBatch> output = omniOperator.getOutput();
        VecBatch outputVecBatch = output.next();

        Object[][] expectedDatas = {{0, 0, 1, 1, 2, 2}, {8L, 8L, 4L, 1L, 5L, 2L}, {3.3D, 6.6D, 2.2D, 5.5D, 1.1D, 4.4D},
                {8L, 8L, 4L, 1L, 5L, 2L}, {1, 2, 1, 2, 1, 2}, {8.0D, 8.0D, 2.5D, 2.5D, 3.5D, 3.5D}};
        assertVecBatchEquals(outputVecBatch, expectedDatas);
        freeVecBatch(outputVecBatch);

        omniOperator.close();
        omniWindowOperatorFactory.close();
    }

    private VecBatch buildData() {
        int rowNum = 6;
        IntVec vec1 = new IntVec(rowNum);
        vec1.set(0, 0);
        vec1.set(1, 1);
        vec1.set(2, 2);
        vec1.set(3, 0);
        vec1.set(4, 1);
        vec1.set(5, 2);
        LongVec vec2 = new LongVec(rowNum);
        vec2.set(0, 8);
        vec2.set(1, 1);
        vec2.set(2, 2);
        vec2.set(3, 8);
        vec2.set(4, 4);
        vec2.set(5, 5);
        DoubleVec vec3 = new DoubleVec(rowNum);
        vec3.set(0, 6.6);
        vec3.set(1, 5.5);
        vec3.set(2, 4.4);
        vec3.set(3, 3.3);
        vec3.set(4, 2.2);
        vec3.set(5, 1.1);
        List<Vec> columns = new ArrayList<>();
        columns.add(vec1);
        columns.add(vec2);
        columns.add(vec3);
        return new VecBatch(columns);
    }
}
