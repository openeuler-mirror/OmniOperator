/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader.testutil;

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcBlockFactory.NestedBlockFactory;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.BooleanColumnReader;
import io.prestosql.orc.reader.ByteColumnReader;
import io.prestosql.orc.reader.ColumnReader;
import io.prestosql.orc.reader.ColumnReaders;
import io.prestosql.orc.reader.FloatColumnReader;
import io.prestosql.orc.reader.ListColumnReader;
import io.prestosql.orc.reader.MapColumnReader;
import io.prestosql.orc.reader.ShortColumnReader;
import io.prestosql.orc.reader.StructColumnReader;
import io.prestosql.orc.reader.TimestampColumnReader;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.reader.OmniDateColumnReader;
import nova.hetu.olk.reader.OmniDecimalColumnReader;
import nova.hetu.olk.reader.OmniDoubleColumnReader;
import nova.hetu.olk.reader.OmniIntColumnReader;
import nova.hetu.olk.reader.OmniLongColumnReader;
import nova.hetu.olk.reader.OmniSliceColumnReader;

import java.util.HashMap;
import java.util.Map;

public class OmniColumnReaders extends ColumnReaders {
    public OmniColumnReaders() {
        super();
    }

    public static ColumnReader createColumnReader(Type type, OrcColumn column,
            AggregatedMemoryContext systemMemoryContext, NestedBlockFactory blockFactory)
            throws OrcCorruptionException {
        switch (column.getColumnType()) {
            case BOOLEAN :
                return new BooleanColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BYTE :
                return new ByteColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case SHORT :
                return new ShortColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case INT :
                return new OmniIntColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()),
                        new HashMap<>());
            case LONG :
                return new OmniLongColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()),
                        new HashMap<>());
            case DATE :
                return new OmniDateColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()),
                        new HashMap<>());
            case FLOAT :
                return new FloatColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case DOUBLE :
                return new OmniDoubleColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()),
                        new HashMap<>());
            case BINARY :
            case STRING :
            case VARCHAR :
            case CHAR :
                return new OmniSliceColumnReader(type, column, systemMemoryContext, new HashMap<>());
            case TIMESTAMP :
                return new TimestampColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case LIST :
                return new ListColumnReader(type, column, systemMemoryContext, blockFactory);
            case STRUCT :
                return new StructColumnReader(type, column, systemMemoryContext, blockFactory);
            case MAP :
                return new MapColumnReader(type, column, systemMemoryContext, blockFactory);
            case DECIMAL :
                return new OmniDecimalColumnReader(type, column,
                        systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()),
                        new HashMap<>());
            case UNION :
            default :
                throw new IllegalArgumentException("Unsupported type: " + column.getColumnType());
        }
    }

    public static ColumnReader createColumnReader(Type type, OrcColumn column,
            AggregatedMemoryContext systemMemoryContext, NestedBlockFactory blockFactory,
            Map<String, String> extensionColumnReadersProperties) throws OrcCorruptionException {
        return createColumnReader(type, column, systemMemoryContext, blockFactory);
    }
}
