/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
import nova.hetu.olk.OmniLocalExecutionPlanner.OmniLocalExecutionPlanContext;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;

import java.util.List;
import java.util.Objects;
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
    private final LoadingCache<ProjectionsCacheKey, OmniProjection> projectionCache;
    private final LoadingCache<FilterCacheKey, OmniPageFilter> filterCache;

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

        // get this from the config
        int expressionCacheSize = 1000;
        projectionCache = CacheBuilder.newBuilder().recordStats().maximumSize(expressionCacheSize)
                .removalListener(notification -> {
                }).build(CacheLoader.from(cacheKey -> new OmniProjection(cacheKey.projections, cacheKey.inputTypes,
                        OmniRowExpressionUtil.Format.JSON)));

        filterCache = CacheBuilder.newBuilder().recordStats().maximumSize(expressionCacheSize)
                .removalListener(notification -> {
                }).build(CacheLoader.from(cacheKey -> {
                    RowExpression re = cacheKey.filter.get();
                    PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(re);
                    OmniPageFilter omniPageFilter = new OmniPageFilter(re, determinismEvaluator.isDeterministic(re),
                            result.getInputChannels(), cacheKey.inputTypes, cacheKey.projections,
                            OmniRowExpressionUtil.Format.JSON);
                    return omniPageFilter;
                }));
    }

    /**
     * Instantiates a new PageProcessor.
     *
     * @param filter the row expression filter
     * @param classNameSuffix (optional) class name suffix used for creating page
     *            processor
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
        return compilePageProcessor(filter, projections, classNameSuffix, initialBatchSize, inputTypes, taskId, null);
    }

    /**
     * Instantiates a new PageProcessor.
     *
     * @param filter the row expression filter
     * @param classNameSuffix (optional) class name suffix used for creating page
     *            processor
     * @param projections the row expression projections
     * @param inputTypes list of input types
     * @param initialBatchSize initial batch size
     * @param taskId the task identifier
     * @param context LocalExecutionPlan context
     * @return Supplier<PageProcessor> supplier page processor
     */
    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter,
            List<? extends RowExpression> projections, Optional<String> classNameSuffix, OptionalInt initialBatchSize,
            List<Type> inputTypes, TaskId taskId, OmniLocalExecutionPlanContext context) {
        VecAllocator vecAllocator = VecAllocatorFactory.get(taskId.getFullId());
        Optional<PageFilter> pageFilter;
        if (filter.isPresent()) {
            OmniPageFilter omniPageFilter = filterCache
                    .getUnchecked(new FilterCacheKey(filter, projections, inputTypes));
            if (!omniPageFilter.isSupported()) {
                return null;
            }
            pageFilter = Optional.of(omniPageFilter);
        } else {
            pageFilter = Optional.empty();
        }

        OmniProjection proj = projectionCache.getUnchecked(new ProjectionsCacheKey(projections, inputTypes));
        if (!proj.isSupported()) {
            return null;
        }

        return () -> new OmniPageProcessor(vecAllocator, pageFilter, proj, initialBatchSize, new ExpressionProfiler(),
                context);
    }

    private static final class FilterCacheKey {
        private final Optional<RowExpression> filter;
        private final List<RowExpression> projections;
        private final List<Type> inputTypes;

        private FilterCacheKey(Optional<RowExpression> filter, List<? extends RowExpression> projections,
                List<Type> inputTypes) {
            this.filter = filter;
            this.inputTypes = inputTypes;
            this.projections = ImmutableList.copyOf(projections);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter, projections, inputTypes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OmniExpressionCompiler.FilterCacheKey other = (OmniExpressionCompiler.FilterCacheKey) obj;
            return Objects.equals(this.filter, other.filter) && Objects.equals(this.projections, other.projections)
                    && Objects.equals(this.inputTypes, other.inputTypes);
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("filter", filter).add("projections", projections)
                    .add("inputTypes", inputTypes).toString();
        }
    }

    private static final class ProjectionsCacheKey {
        private final List<RowExpression> projections;
        private final List<Type> inputTypes;

        private ProjectionsCacheKey(List<? extends RowExpression> projections, List<Type> inputTypes) {
            this.inputTypes = inputTypes;
            this.projections = ImmutableList.copyOf(projections);
        }

        @Override
        public int hashCode() {
            return Objects.hash(projections, inputTypes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OmniExpressionCompiler.ProjectionsCacheKey other = (OmniExpressionCompiler.ProjectionsCacheKey) obj;
            return Objects.equals(this.projections, other.projections)
                    && Objects.equals(this.inputTypes, other.inputTypes);
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("projections", projections).add("inputTypes", inputTypes).toString();
        }
    }
}
