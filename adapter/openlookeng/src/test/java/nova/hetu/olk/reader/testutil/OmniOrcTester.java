/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader.testutil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.orc.FileOrcDataSource;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.OrcTester;
import io.prestosql.orc.TempFile;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterators.advance;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.TestingOrcPredicate.createOrcPredicate;
import static io.prestosql.orc.metadata.CompressionKind.ZLIB;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class OmniOrcTester extends OrcTester {
    public static OmniOrcTester quickOrcTester() {
        OmniOrcTester omniOrcTester = new OmniOrcTester();
        omniOrcTester.structTestsEnabled = true;
        omniOrcTester.mapTestsEnabled = true;
        omniOrcTester.listTestsEnabled = true;
        omniOrcTester.nullTestsEnabled = true;
        omniOrcTester.missingStructFieldsTestsEnabled = true;
        omniOrcTester.skipBatchTestsEnabled = true;
        omniOrcTester.formats = ImmutableSet.of(Format.ORC_12);
        omniOrcTester.compressions = ImmutableSet.of(ZLIB);
        return omniOrcTester;
    }

    @Override
    public void assertFileContentsPresto(Type type, TempFile tempFile, List<?> expectedValues, boolean skipFirstBatch,
            boolean skipStripe, boolean useSelectiveOrcReader, List<Map<Integer, TupleDomainFilter>> filters)
            throws IOException {
        OrcPredicate orcPredicate = createOrcPredicate(type, expectedValues);
        if (useSelectiveOrcReader) {
            assertFileContentsPresto(ImmutableList.of(type), tempFile, expectedValues, orcPredicate, Optional.empty());
            for (Map<Integer, TupleDomainFilter> columnFilters : filters) {
                assertFileContentsPresto(ImmutableList.of(type), tempFile,
                        filterRows(ImmutableList.of(type), expectedValues, columnFilters), orcPredicate,
                        Optional.of(columnFilters));
            }
            return;
        }

        try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile,
                createOrcPredicate(type, expectedValues), type, MAX_BATCH_SIZE)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            boolean isFirst = true;
            int rowsProcessed = 0;
            Iterator<?> iterator = expectedValues.iterator();
            for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
                int batchSize = page.getPositionCount();
                if (skipStripe && rowsProcessed < 10000) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                } else if (skipFirstBatch && isFirst) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                    isFirst = false;
                } else {
                    Block block = page.getBlock(0);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int i = 0; i < batchSize; i++) {
                        assertTrue(iterator.hasNext());
                        Object expected = iterator.next();
                        Object actual = data.get(i);
                        assertColumnValueEquals(type, actual, expected);
                    }
                }
                assertEquals(recordReader.getReaderPosition(), rowsProcessed);
                assertEquals(recordReader.getFilePosition(), rowsProcessed);
                rowsProcessed += batchSize;
            }
            assertFalse(iterator.hasNext());
            assertNull(recordReader.nextPage());

            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, Type type,
            int initialBatchSize) throws IOException {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OmniOrcReader omniOrcReader = new OmniOrcReader(orcDataSource, new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE), MAX_BLOCK_SIZE);

        assertEquals(omniOrcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(omniOrcReader.getFooter().getRowsInRowGroup(), 10_000);

        return omniOrcReader.createRecordReader(omniOrcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(type), predicate, HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(),
                initialBatchSize, RuntimeException::new);
    }
}
