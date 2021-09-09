/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import static io.prestosql.spi.type.Chars.isCharType;

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.SliceColumnReader;
import io.prestosql.spi.type.Type;

/**
 * The type Omni slice column reader.
 *
 * @since 20210630
 */
public class OmniSliceColumnReader extends SliceColumnReader {
    /**
     * Instantiates a new Omni slice column reader.
     *
     * @param type the type
     * @param column the column
     * @param systemMemoryContext the system memory context
     * @throws OrcCorruptionException the orc corruption exception
     */
    public OmniSliceColumnReader(Type type, OrcColumn column, AggregatedMemoryContext systemMemoryContext)
        throws OrcCorruptionException {
        super(type, column, systemMemoryContext);

        int maxCodePointCount = getMaxCodePointCount(type);
        boolean charType = isCharType(type);
        directReader = new OmniSliceDirectColumnReader(column, maxCodePointCount, charType);
        dictionaryReader = new OmniSliceDictionaryColumnReader(column,
            systemMemoryContext.newLocalMemoryContext(SliceColumnReader.class.getSimpleName()), maxCodePointCount,
            charType);
    }
}
