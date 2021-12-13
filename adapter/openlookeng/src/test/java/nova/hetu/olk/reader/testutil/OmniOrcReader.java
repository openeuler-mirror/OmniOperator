/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader.testutil;

import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCacheProperties;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcFileTail;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class OmniOrcReader extends OrcReader {
    public OmniOrcReader(OrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize tinyStripeThreshold,
            DataSize maxBlockSize) throws IOException {
        super(orcDataSource, maxMergeDistance, tinyStripeThreshold, maxBlockSize);
    }

    public OmniOrcReader(OrcDataSource orcDataSource, OrcFileTail fileTail, DataSize maxMergeDistance,
            DataSize tinyStripeThreshold, DataSize maxBlockSize) {
        super(orcDataSource, fileTail, maxMergeDistance, tinyStripeThreshold, maxBlockSize);
    }

    @Override
    public OrcRecordReader createRecordReader(List<OrcColumn> readColumns, List<Type> readTypes, OrcPredicate predicate,
            long offset, long length, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize, Function<Exception, RuntimeException> exceptionTransform,
            Optional<List<IndexMetadata>> indexes, SplitMetadata splitMetadata, Map<String, Domain> domains,
            OrcCacheStore orcCacheStore, OrcCacheProperties orcCacheProperties, boolean pageMetadataEnabled,
            Map<String, String> extensionColumnReadersProperties) throws OrcCorruptionException {
        return new OmniOrcRecordReader(requireNonNull(readColumns, "readColumns is null"),
                requireNonNull(readTypes, "readTypes is null"), requireNonNull(predicate, "predicate is null"),
                getFooter().getNumberOfRows(), getFooter().getStripes(), getFooter().getFileStats(),
                getMetadata().getStripeStatsList(), getOrcDataSource(), offset, length, getFooter().getTypes(),
                getDecompressor(), getFooter().getRowsInRowGroup(),
                requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null"), getHiveWriterVersion(),
                getMetadataReader(), getMaxMergeDistance(), getTinyStripeThreshold(), getMaxBlockSize(),
                getFooter().getUserMetadata(), systemMemoryUsage, getWriteValidation(), initialBatchSize,
                exceptionTransform, indexes, splitMetadata, domains, orcCacheStore, orcCacheProperties,
                pageMetadataEnabled, extensionColumnReadersProperties);
    }
}
