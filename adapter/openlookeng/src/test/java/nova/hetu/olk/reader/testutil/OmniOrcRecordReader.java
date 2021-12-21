/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader.testutil;

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcBlockFactory;
import io.prestosql.orc.OrcCacheProperties;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDecompressor;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.OrcWriteValidation;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.MetadataReader;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.PostScript;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.StripeStatistics;
import io.prestosql.orc.reader.ColumnReader;
import io.prestosql.orc.reader.ColumnReaders;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static nova.hetu.olk.reader.testutil.OmniColumnReaders.createColumnReader;

public class OmniOrcRecordReader extends OrcRecordReader {
    public OmniOrcRecordReader(List<OrcColumn> readColumns, List<Type> readTypes, OrcPredicate predicate,
            long numberOfRows, List<StripeInformation> fileStripes,
            Optional<ColumnMetadata<ColumnStatistics>> fileStats, List<Optional<StripeStatistics>> stripeStats,
            OrcDataSource orcDataSource, long splitOffset, long splitLength, ColumnMetadata<OrcType> orcTypes,
            Optional<OrcDecompressor> decompressor, int rowsInRowGroup, DateTimeZone hiveStorageTimeZone,
            PostScript.HiveWriterVersion hiveWriterVersion, MetadataReader metadataReader, DataSize maxMergeDistance,
            DataSize tinyStripeThreshold, DataSize maxBlockSize, Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage, Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize, Function<Exception, RuntimeException> exceptionTransform,
            Optional<List<IndexMetadata>> indexes, SplitMetadata splitMetadata, Map<String, Domain> domains,
            OrcCacheStore orcCacheStore, OrcCacheProperties orcCacheProperties, boolean pageMetadataEnabled,
            Map<String, String> extensionColumnReadersProperties) throws OrcCorruptionException {
        super(readColumns, readTypes, predicate, numberOfRows, fileStripes, fileStats, stripeStats, orcDataSource,
                splitOffset, splitLength, orcTypes, decompressor, rowsInRowGroup, hiveStorageTimeZone,
                hiveWriterVersion, metadataReader, maxMergeDistance, tinyStripeThreshold, maxBlockSize, userMetadata,
                systemMemoryUsage, writeValidation, initialBatchSize, exceptionTransform, indexes, splitMetadata,
                domains, orcCacheStore, orcCacheProperties, pageMetadataEnabled, extensionColumnReadersProperties);

        setColumnReadersParam(createColumnReaders(readColumns, readTypes,
                systemMemoryUsage.newAggregatedMemoryContext(), new OrcBlockFactory(exceptionTransform, true),
                orcCacheStore, orcCacheProperties, extensionColumnReadersProperties));
    }

    private ColumnReader[] createColumnReaders(List<OrcColumn> columns, List<Type> readTypes,
            AggregatedMemoryContext systemMemoryContext, OrcBlockFactory blockFactory, OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties, Map<String, String> extensionColumnReadersProperties)
            throws OrcCorruptionException {
        ColumnReader[] columnReaders = new ColumnReader[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int columnIndex = i;
            Type readType = readTypes.get(columnIndex);
            OrcColumn column = columns.get(columnIndex);
            ColumnReader columnReader = createColumnReader(readType, column, systemMemoryContext,
                    blockFactory.createNestedBlockFactory(block -> blockLoaded(columnIndex, block)),
                    extensionColumnReadersProperties);
            if (orcCacheProperties.isRowDataCacheEnabled()) {
                columnReader = ColumnReaders.wrapWithCachingStreamReader(columnReader, column,
                        orcCacheStore.getRowDataCache());
            }
            columnReaders[columnIndex] = columnReader;
        }
        return columnReaders;
    }
}
