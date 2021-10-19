/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu.olk.operator.benchmark;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.VarcharType;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.lang.Float.floatToRawIntBits;

public final class BlockUtil {
    private BlockUtil() {
    }

    public static Block createStringSequenceBlock(int start, int end, VarcharType type) {
        BlockBuilder builder = type.createBlockBuilder(null, 100);

        for (int i = start; i < end; i++) {
            type.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createIntegerSequenceBlock(int start, int end) {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            INTEGER.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createStringDictionaryBlock(int start, int length, VarcharType type) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeString(builder, String.valueOf(i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createLongDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = BIGINT.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            BIGINT.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createIntegerDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = INTEGER.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            INTEGER.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createRealDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = REAL.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            REAL.writeLong(builder, floatToRawIntBits((float) i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createDoubleDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            DOUBLE.writeDouble(builder, (double) i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createBooleanDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            BOOLEAN.writeBoolean(builder, i % 2 == 0);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createDateDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = DATE.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            DATE.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createTimestampDictionaryBlock(int start, int length) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = TIMESTAMP.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            TIMESTAMP.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createShortDecimalDictionaryBlock(int start, int length, DecimalType type) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        long base = BigInteger.TEN.pow(type.getScale()).longValue();

        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeLong(builder, base * i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createLongDecimalDictionaryBlock(int start, int length, DecimalType type) {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BigInteger base = BigInteger.TEN.pow(type.getScale());

        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeSlice(builder, encodeUnscaledValue(BigInteger.valueOf(i).multiply(base)));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }
}
