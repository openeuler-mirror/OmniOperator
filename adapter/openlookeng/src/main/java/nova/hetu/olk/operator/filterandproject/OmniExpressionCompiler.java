/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;

import io.prestosql.execution.TaskId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.project.PageFieldsToInputParametersRewriter;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.ExpressionProfiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.DeterminismEvaluator;
import io.prestosql.sql.relational.RowExpression;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import javax.inject.Inject;

/**
 * The type Omni expression compiler.
 *
 * @since 20210630
 */
public class OmniExpressionCompiler extends ExpressionCompiler {
    private final DeterminismEvaluator determinismEvaluator;

    /**
     * Instantiates a new Omni expression compiler.
     *
     * @param metadata the metadata
     * @param pageFunctionCompiler the page function compiler
     */
    @Inject
    public OmniExpressionCompiler(Metadata metadata, PageFunctionCompiler pageFunctionCompiler) {
        super(metadata, pageFunctionCompiler);
        this.determinismEvaluator = new DeterminismEvaluator(metadata);
    }

    /**
     * Instantiates a new PageProcessor.
     *
     * @param filter the row expression filter
     * @param classNameSuffix (optional) class name suffix used for creating page processor
     * @param projections the row expression projections
     * @param inputTypes list of input types
     * @param initialBatchSize initial batch size
     * @param taskId the task identifier
     * @return Supplier<PageProcessor> supplier page processor
     */
    @Override
    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter,
            List<? extends RowExpression> projections, Optional<String> classNameSuffix, OptionalInt initialBatchSize,
            List<Type> inputTypes, TaskId taskId) {
        VecAllocator vecAllocator = VecAllocatorFactory.get(taskId.getFullId());
        OmniProjection proj = new OmniProjection(projections, inputTypes, filter);
        Optional<PageFilter> pageFilter;
        if (filter.isPresent()) {
            RowExpression re = filter.get();
            PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(re);
            pageFilter = Optional.of(
                new OmniPageFilter(re, determinismEvaluator.isDeterministic(re), result.getInputChannels(), inputTypes,
                    projections));
        } else {
            pageFilter = Optional.empty();
        }
        return () -> new OmniPageProcessor(vecAllocator, pageFilter, proj, initialBatchSize, new ExpressionProfiler());
    }

    /**
     * Instantiates a new OmniPageProcessor
     *
     * @param filter the row expression filter
     * @param projections the row expression projections
     * @param inputTypes list of input types
     * @param taskId the task identifier
     * @return Supplier<OmniPageProcessor> supplier omni page processor
     */
    public Supplier<OmniPageProcessor> getOmniPageProcessor(Optional<RowExpression> filter,
        List<? extends RowExpression> projections, List<Type> inputTypes, TaskId taskId) {
        VecAllocator vecAllocator = VecAllocatorFactory.get(taskId.getFullId());
        Optional<PageFilter> pageFilter;
        OmniProjection proj = new OmniProjection(projections, inputTypes, filter);
        if (filter.isPresent()) {
            RowExpression re = filter.get();
            PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(re);
            pageFilter = Optional.of(
                new OmniPageFilter(re, determinismEvaluator.isDeterministic(re), result.getInputChannels(), inputTypes,
                    projections));
        } else {
            pageFilter = Optional.empty();
        }
        return () -> new OmniPageProcessor(vecAllocator, pageFilter, proj, OptionalInt.empty(), new ExpressionProfiler());
    }
}
