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
package nova.hetu.omniruntime.operator;

import nova.hetu.omniruntime.operator.aggregator.JOmniHashAggregationOperator;
import nova.hetu.omniruntime.utils.OmniUtils;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static nova.hetu.omniruntime.utils.OmniUtils.getRowNumbers;
import static nova.hetu.omniruntime.utils.OmniUtils.transformVecAddress;

public class JFilterAndProjectOperator
        extends JOmniOperator
{
    JniWrapper jniWrapper = new JniWrapper();

    public JFilterAndProjectOperator(JniWrapper jniWrapper, long nativeOperator)
    {
        super(jniWrapper, nativeOperator);
    }

    @Override
    public int addInput(List<Vec> vecs, int rowNumber)
    {
        int vecCount = vecs.size();
        LongVec vecAddresses = transformVecAddress(vecs);
        jniWrapper.addInput(this.getNativeOperator(), vecAddresses.getAddress(), vecCount, getRowNumbers(vecs, vecCount).getAddress(), rowNumber);
        return 0;
    }

    @Override
    public int addInput(List<Vec> data, int[] positionCounts)
    {
        return 0;
    }

    @Override
    public OMResult[] getOutput()
    {
        return getJniWrapper().getOutput(getNativeOperator());
    }

    public void finished(FilterContext filterContext)
    {
        //TODO:Do Release filter module off-heap resource
        jniWrapper.filterFinished(filterContext.getFilterId());
        filterContext.finished();
    }

    public static class JFilterAndProjectOperatorFactory
            extends JOmniOperatorFactory
    {
        public JFilterAndProjectOperatorFactory(long nativeOperatorFactory)
        {
            super(nativeOperatorFactory);
        }

        public static JFilterAndProjectOperatorFactory create(String filterExpression, VecType[] inputTypes, int[] projects)
        {
            // compile and optimized
            Integer hashKey = Objects.hash(filterExpression, Arrays.hashCode(inputTypes), Arrays.hashCode(projects));
            Long nativeOperatorFactory = getOmniFactoryCache().getIfPresent(hashKey);
            if (nativeOperatorFactory == null) {
                nativeOperatorFactory = getJniWrapper().createFilterAndProjectOperatorFactory(
                        filterExpression, OmniUtils.transformVecType(inputTypes), inputTypes.length, projects, projects.length);
                if (nativeOperatorFactory == null) {
                    throw new RuntimeException("create nativeOperatorFactory failed");
                }
                getOmniFactoryCache().put(hashKey, nativeOperatorFactory);
            }
            return new JFilterAndProjectOperatorFactory(nativeOperatorFactory);
        }

        @Override
        public JOmniOperator createOmniOperator()
        {
            JniWrapper jniWrapper = getJniWrapper();
            long nativeOperator = jniWrapper.createOperator(getNativeOperatorFactory());
            return new JOmniHashAggregationOperator(jniWrapper, nativeOperator);
        }
    }
}
