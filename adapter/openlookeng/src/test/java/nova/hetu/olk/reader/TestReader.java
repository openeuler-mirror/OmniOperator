/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import com.google.common.base.Strings;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import nova.hetu.olk.reader.testutil.OmniOrcTester;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.stream.Collectors.toList;

@Test
public class TestReader {
    private static final int CHAR_LENGTH = 10;
    private static OmniOrcTester tester = OmniOrcTester.quickOrcTester();
    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);
    private static final CharType CHAR = createCharType(CHAR_LENGTH);

    @Test
    public void testDoubleSequence() throws Exception {
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    public void testDecimalSequence() throws Exception {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_2, decimalSequence("-30", "1", 60, 2, 1));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "1", 60_00, 4, 2));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_8, decimalSequence("-3000000", "100", 60_000, 8, 4));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_17, decimalSequence("-30000000000", "1000000", 60_000, 17, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, decimalSequence("-30000000000", "1000000", 60_000, 18, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38,
                decimalSequence("-3000000000000000000", "100000000000000", 60_000, 38, 16));

        Random random = new Random(0);
        List<SqlDecimal> values = new ArrayList<>();
        values.add(new SqlDecimal(new BigInteger(Strings.repeat("9", 18)), DECIMAL_TYPE_PRECISION_18.getPrecision(),
                DECIMAL_TYPE_PRECISION_18.getScale()));
        values.add(new SqlDecimal(new BigInteger("-" + Strings.repeat("9", 18)),
                DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        BigInteger nextValue = BigInteger.ONE;
        for (int i = 0; i < 59; i++) {
            values.add(new SqlDecimal(nextValue, DECIMAL_TYPE_PRECISION_18.getPrecision(),
                    DECIMAL_TYPE_PRECISION_18.getScale()));
            values.add(new SqlDecimal(nextValue.negate(), DECIMAL_TYPE_PRECISION_18.getPrecision(),
                    DECIMAL_TYPE_PRECISION_18.getScale()));
            nextValue = nextValue.multiply(BigInteger.valueOf(2));
        }
        for (int i = 0; i < 100_000; ++i) {
            BigInteger value = new BigInteger(59, random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            values.add(new SqlDecimal(value, DECIMAL_TYPE_PRECISION_18.getPrecision(),
                    DECIMAL_TYPE_PRECISION_18.getScale()));
        }
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, values);

        random = new Random(0);
        values = new ArrayList<>();
        values.add(new SqlDecimal(new BigInteger(Strings.repeat("9", 38)), DECIMAL_TYPE_PRECISION_38.getPrecision(),
                DECIMAL_TYPE_PRECISION_38.getScale()));
        values.add(new SqlDecimal(new BigInteger("-" + Strings.repeat("9", 38)),
                DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        nextValue = BigInteger.ONE;
        for (int i = 0; i < 127; i++) {
            values.add(new SqlDecimal(nextValue, 38, 16));
            values.add(new SqlDecimal(nextValue.negate(), 38, 16));
            nextValue = nextValue.multiply(BigInteger.valueOf(2));
        }
        for (int i = 0; i < 100_000; ++i) {
            BigInteger value = new BigInteger(126, random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            values.add(new SqlDecimal(value, DECIMAL_TYPE_PRECISION_38.getPrecision(),
                    DECIMAL_TYPE_PRECISION_38.getScale()));
        }
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, values);
    }

    @Test
    public void testCharDirectSequence() throws Exception {
        tester.testRoundTrip(CHAR, intsBetween(0, 30_000).stream().map(this::toCharValue).collect(toList()));
    }

    @Test
    public void testCharDictionarySequence() throws Exception {
        tester.testRoundTrip(CHAR, newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                .map(this::toCharValue).collect(toList()));
    }

    @Test
    public void testLongSequence() throws Exception {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values) throws Exception {
        List<Long> writeValues = ImmutableList.copyOf(values).stream().map(Number::longValue).collect(toList());

        tester.testRoundTrip(INTEGER, writeValues.stream().map(Long::intValue) // truncate values to int range
                .collect(toList()));

        tester.testRoundTrip(BIGINT, writeValues);

        tester.testRoundTrip(DATE, writeValues.stream().map(Long::intValue).map(SqlDate::new).collect(toList()));
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive) {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static List<Double> doubleSequence(double start, double step, int items) {
        List<Double> values = new ArrayList<>();
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
    }

    private static List<SqlDecimal> decimalSequence(String start, String step, int items, int precision, int scale) {
        BigInteger decimalStep = new BigInteger(step);

        List<SqlDecimal> values = new ArrayList<>();
        BigInteger nextValue = new BigInteger(start);
        for (int i = 0; i < items; i++) {
            values.add(new SqlDecimal(nextValue, precision, scale));
            nextValue = nextValue.add(decimalStep);
        }
        return values;
    }

    private String toCharValue(Object value) {
        return Strings.padEnd(value.toString(), CHAR_LENGTH, ' ');
    }
}
