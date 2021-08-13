/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;

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

    @Override
    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter,
        List<? extends RowExpression> projections, Optional<String> classNameSuffix, OptionalInt initialBatchSize,
        List<Type> inputTypes) {
        OmniProjection proj = new OmniProjection(projections, inputTypes, filter);
        Optional<PageFilter> pageFilter;
        if (filter.isPresent()) {
            RowExpression re = filter.get();
            PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(re);
            int[] projects = result.getInputChannels()
                .getInputChannels()
                .stream()
                .mapToInt(Integer::intValue)
                .toArray();
            pageFilter = Optional.of(
                new OmniPageFilter(re, determinismEvaluator.isDeterministic(re), result.getInputChannels(), inputTypes,
                    proj.getNeededCols()));
        } else {
            pageFilter = Optional.empty();
        }
        return () -> {
            return new OmniPageProcessor(pageFilter, proj, initialBatchSize, new ExpressionProfiler());
        };
    }
}
