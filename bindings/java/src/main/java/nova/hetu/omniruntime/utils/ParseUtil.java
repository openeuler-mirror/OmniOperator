/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseUtil {
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");

    private ParseUtil() {
    }

    /**
     *
     * parse memory size to byte, like 1B, 1K, 1M, 1G
     *
     * @param size capacity size with unit
     * @return size in bytes
     */
    public static long parserMemoryParameters(String size) {
        Matcher matcher = PATTERN.matcher(size);
        if (!matcher.matches()) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_PARAM_ERROR,
                    "size is not a valid data size string" + size);
        }

        long value = Long.parseLong(matcher.group(1));
        String unitString = matcher.group(2);

        for (Unit unit : Unit.values()) {
            if (unit.getUnitString().equals(unitString)) {
                return value * unit.getFactor();
            }
        }

        throw new OmniRuntimeException(OmniErrorType.OMNI_PARAM_ERROR, "Unknown unit:" + unitString);
    }

    enum Unit
    {
        BYTE(1L, "B"),
        KILOBYTE(1L << 10, "K"),
        MEGABYTE(1L << 20, "M"),
        GIGABYTE(1L << 30, "G"),
        TERABYTE(1L << 40, "T");

        private final long factor;
        private final String unitString;

        Unit(long factor, String unitString) {
            this.factor = factor;
            this.unitString = unitString;
        }

        long getFactor() {
            return factor;
        }

        String getUnitString() {
            return unitString;
        }
    }
}
