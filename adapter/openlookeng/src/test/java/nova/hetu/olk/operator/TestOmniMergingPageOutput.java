/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
package nova.hetu.olk.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.filterandproject.OmniMergingPageOutput;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterators.transform;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOmniMergingPageOutput {

    @Test
    public void testRowBlock() {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        List<RowType.Field> fields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            fields.add(new RowType.Field(Optional.of(i + ""), types.get(i)));
        }
        RowType rowType = RowType.from(fields);

        OmniMergingPageOutput operator = createOmniMergingPageOutput(ImmutableList.of(rowType), 1024, 10);
        assertFalse(operator.isFinished());
        List<Object> values1 = new ArrayList<>();
        values1.add(1);
        values1.add(1.1);
        List<Object> values11 = new ArrayList<>();
        values11.add(2);
        values11.add(2.2);

        List<Object> values2 = new ArrayList<>();
        values2.add(3);
        values2.add(3.3);
        List<Object> values22 = new ArrayList<>();
        values22.add(4);
        values22.add(4.4);
        List<Page> input = rowPagesBuilder(rowType).row(values1).row(values11).pageBreak().row(values2).row(values22).build();

        operator.addInput(createPagesIterator(input));

        assertNull(operator.getOutput());
        assertFalse(operator.isFinished());
        operator.finish();
        assertFalse(operator.isFinished());

        Page expected = rowPagesBuilder(rowType).row(values1).row(values11).row(values2).row(values22).build().get(0);
        Page result = operator.getOutput();
        assertPageEquals(ImmutableList.of(rowType), result, expected);
        assertTrue(operator.isFinished());
        result.close();
    }

    private OmniMergingPageOutput createOmniMergingPageOutput(List<Type> sourceTypes, long minPageSizeInBytes, int minRowCount) {
        return new OmniMergingPageOutput(sourceTypes, minPageSizeInBytes, minRowCount);
    }

    private Iterator<Optional<Page>> createPagesIterator(List<Page> pages) {
        return transform(pages.iterator(), Optional::of);
    }
}