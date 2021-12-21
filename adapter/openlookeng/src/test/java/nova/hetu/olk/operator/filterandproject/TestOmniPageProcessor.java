package nova.hetu.olk.operator.filterandproject;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.InputChannels;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.function.Signature;
import io.prestosql.sql.gen.ExpressionProfiler;
import io.prestosql.sql.relational.CallExpression;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;

import org.testng.annotations.Test;

public class TestOmniPageProcessor {
    @Test
    public void testSelectNoneFilterLazyLoad() {

        Signature greatThan = internalOperator(GREATER_THAN, BOOLEAN, ImmutableList.of(INTEGER, INTEGER));
        CallExpression filterExpression = new CallExpression(greatThan, BOOLEAN,
            ImmutableList.of(field(1, INTEGER), constant(10L, BIGINT)));

        Signature addOne = internalOperator(ADD, INTEGER, ImmutableList.of(INTEGER, INTEGER));
        CallExpression projectExpression = new CallExpression(addOne, INTEGER,
            ImmutableList.of(field(0, INTEGER), constant(1, INTEGER)));

        OmniPageFilter filter = new OmniPageFilter(filterExpression, true, new InputChannels(1),
            ImmutableList.of(INTEGER, INTEGER), ImmutableList.of(projectExpression),
            OmniRowExpressionUtil.Format.JSON);
        OmniProjection projection = new OmniProjection(ImmutableList.of(projectExpression),
            ImmutableList.of(INTEGER, INTEGER), OmniRowExpressionUtil.Format.JSON);

        VecAllocator vecAllocator = VecAllocatorFactory.create(TestOmniPageProcessor.class.getName(), null);
        PageProcessor pageProcessor = new OmniPageProcessor(vecAllocator, Optional.of(filter), projection,
            OptionalInt.empty(), new ExpressionProfiler(), null);

        // if channel 1 is loaded, test will fail
        int ROW_NUM = 20;
        Block column1 = new LazyBlock(ROW_NUM, lazyBlock -> {
            IntVec vec = new IntVec(vecAllocator, ROW_NUM);
            for (int i = 0; i < 20; i++) {
                vec.set(i, i);
            }
            lazyBlock.setBlock(new IntArrayOmniBlock(ROW_NUM, vec));
        });
        Block column2 = new LazyBlock(ROW_NUM, lazyBlock -> {
            IntVec vec = new IntVec(vecAllocator, ROW_NUM);
            for (int i = 0; i < 20; i++) {
                vec.set(i, i);
            }
            lazyBlock.setBlock(new IntArrayOmniBlock(ROW_NUM, vec));
        });
        Page inputPage = new Page(column1, column2);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(
            PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, new DriverYieldSignal(), memoryContext,
            inputPage);
        assertEquals(memoryContext.getBytes(), 0);
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        outputPages.forEach(outputPage -> {
            if (outputPage.isPresent()) {
                outputPage.get().close();
            }
        });
        vecAllocator.close();
    }
}
