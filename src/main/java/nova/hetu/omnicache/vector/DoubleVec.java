/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Representing a floating point number
 */
public class DoubleVec extends Vec<Double> {

    private OMVectorBase base = new OMVectorBase();

    public DoubleVec(int size) {
        super(size, size * Double.BYTES);
    }

    public DoubleVec(ByteBuffer buffer, int size) {
        super(buffer, size);
    }

    @Override
    public Vec hash() {
        return null;
    }

    @Override
    public Vec mul(Double other) {
        base.mul(OMVectorBase.DOUBLE_DATA_TYPE, data, other.intValue());
        return null;
    }

    @Override
    public Vec mmul(Vec<Double> other) {
        return null;
    }

    @Override
    public Vec filter() {
        return null;
    }

    @Override
    public void set(int idx, Double value) {
        data.putDouble(idx * Double.BYTES, value);
    }

    @Override
    public DoubleVec slice(int startIdx, int endIdx) {
        byte[] regionData = new byte[(endIdx - startIdx) * Double.BYTES];
        DoubleVec newVec = new DoubleVec(regionData.length);
        data.reset();
        data.get(regionData, 0, regionData.length);
        newVec.data.put(regionData);
        return newVec;
    }

    @Override
    public void addValues(Double[] values) {
        for (Double val : values) {
            data.putDouble(val);
        }
    }

    @Override
    public Double get(int idx) {
        return data.getDouble(idx * Double.BYTES);
    }

    @Override
    public Vec groupby() {
        return null;
    }

    @Override
    public Vec join(Vec other) {
        return null;
    }

    @Override
    public VecType getType() {
        return VecType.DOUBLE;
    }

    @Override
    public Vec concat(Vec other) {
        ByteBuffer newBuffer = OMVectorBase.concat(this.data, other.data, this.size * Double.BYTES, other.size * Double.BYTES);
        newBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return new DoubleVec(newBuffer, this.size + other.size);
    }
}