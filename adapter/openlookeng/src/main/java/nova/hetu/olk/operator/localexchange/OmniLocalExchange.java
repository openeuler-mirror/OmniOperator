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

package nova.hetu.olk.operator.localexchange;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;

import io.airlift.units.DataSize;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.PageReference;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PartitioningHandle;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class OmniLocalExchange extends LocalExchange {
    public OmniLocalExchange(int sinkFactoryCount, int bufferCount, PartitioningHandle partitioning,
            List<? extends Type> types, List<Integer> partitionChannels, Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes) {
        super(sinkFactoryCount, bufferCount, partitioning, types, partitionChannels, partitionHashChannel,
                maxBufferedBytes);

        List<Consumer<PageReference>> buffers = this.sources.stream()
                .map(buffer -> (Consumer<PageReference>) buffer::addPage).collect(toImmutableList());

        if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            exchangerSupplier = () -> new OmniPartitioningExchanger(buffers, this.memoryManager, types,
                    partitionChannels, partitionHashChannel);
        }
    }

    @ThreadSafe
    public static class OmniLocalExchangeFactory extends LocalExchangeFactory {
        public OmniLocalExchangeFactory(PartitioningHandle partitioning, int defaultConcurrency, List<Type> types,
                List<Integer> partitionChannels, Optional<Integer> partitionHashChannel,
                PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy, DataSize maxBufferedBytes) {
            super(partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel,
                    exchangeSourcePipelineExecutionStrategy, maxBufferedBytes);
        }

        public synchronized LocalExchange getLocalExchange(Lifespan lifespan) {
            if (exchangeSourcePipelineExecutionStrategy == UNGROUPED_EXECUTION) {
                checkArgument(lifespan.isTaskWide(),
                        "OmniLocalExchangeFactory is declared as UNGROUPED_EXECUTION. Driver-group exchange cannot be created.");
            } else {
                checkArgument(!lifespan.isTaskWide(),
                        "OmniLocalExchangeFactory is declared as GROUPED_EXECUTION. Task-wide exchange cannot be created.");
            }
            return localExchangeMap.computeIfAbsent(lifespan, ignored -> {
                checkState(noMoreSinkFactories);
                LocalExchange localExchange = new OmniLocalExchange(numSinkFactories, bufferCount, partitioning, types,
                        partitionChannels, partitionHashChannel, maxBufferedBytes);
                for (LocalExchangeSinkFactoryId closedSinkFactoryId : closedSinkFactories) {
                    localExchange.getSinkFactory(closedSinkFactoryId).close();
                }
                return localExchange;
            });
        }
    }
}
