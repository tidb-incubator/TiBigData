/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hybrid source that switches underlying sources based on configured source chain.
 *
 * <pre>{@code
 * FileSource<String> fileSource = null;
 * HybridSource<String> hybridSource =
 *     new HybridSourceBuilder<String, ContinuousFileSplitEnumerator>()
 *         .addSource(fileSource) // fixed start position
 *         .addSource(
 *             (enumerator) -> {
 *               // instantiate Kafka source based on enumerator
 *               KafkaSource<String> kafkaSource = createKafkaSource(enumerator);
 *               return kafkaSource;
 *             }, Boundedness.CONTINUOUS_UNBOUNDED)
 *         .build();
 * }</pre>
 */
@PublicEvolving
public class HybridSource<T> implements Source<T, HybridSourceSplit, HybridSourceEnumeratorState> {

    private final List<SourceListEntry> sources;
    // sources are populated per subtask at switch time
    private final Map<Integer, Source> switchedSources;

    /** Protected for subclass, use {@link #builder(Source)} to construct source. */
    protected HybridSource(List<SourceListEntry> sources) {
        Preconditions.checkArgument(!sources.isEmpty());
        for (int i = 0; i < sources.size() - 1; i++) {
            Preconditions.checkArgument(
                    Boundedness.BOUNDED.equals(sources.get(i).boundedness),
                    "All sources except the final source need to be bounded.");
        }
        this.sources = sources;
        this.switchedSources = new HashMap<>(sources.size());
    }

    /** Builder for {@link HybridSource}. */
    public static <T, EnumT extends SplitEnumerator> HybridSourceBuilder<T, EnumT> builder(
            Source<T, ?, ?> firstSource) {
        HybridSourceBuilder<T, EnumT> builder = new HybridSourceBuilder<>();
        return builder.addSource(firstSource);
    }

    @Override
    public Boundedness getBoundedness() {
        return sources.get(sources.size() - 1).boundedness;
    }

    @Override
    public SourceReader<T, HybridSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new HybridSourceReader(readerContext, switchedSources);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext) {
        return new HybridSourceSplitEnumerator(enumContext, sources, 0, switchedSources);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext,
            HybridSourceEnumeratorState checkpoint)
            throws Exception {
        // TODO: restore underlying enumerator
        return new HybridSourceSplitEnumerator(
                enumContext, sources, checkpoint.getCurrentSourceIndex(), switchedSources);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        return new HybridSourceSplitSerializer(switchedSources);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new HybridSourceEnumeratorStateSerializer(switchedSources);
    }

    /**
     * Factory for underlying sources of {@link HybridSource}.
     *
     * <p>This factory permits building of a source at graph construction time or deferred at switch
     * time. Provides the ability to set a start position in any way a specific source allows.
     * Future convenience could be built on top of it, for example a default implementation that
     * recognizes optional interfaces to transfer position in a universal format.
     *
     * <p>Called when the current enumerator has finished and before the next enumerator is created.
     * The enumerator end state can thus be used to set the next source's start start position. Only
     * required for dynamic position transfer at time of switching.
     *
     * <p>If start position is known at job submission time, the source can be constructed in the
     * entry point and simply wrapped into the factory, providing the benefit of validation during
     * submission.
     */
    public interface SourceFactory<T, SourceT extends Source, FromEnumT extends SplitEnumerator>
            extends Serializable {
        SourceT create(FromEnumT enumerator);
    }

    private static class PassthroughSourceFactory<
                    T, SourceT extends Source<T, ?, ?>, FromEnumT extends SplitEnumerator>
            implements SourceFactory<T, SourceT, FromEnumT> {

        private final SourceT source;

        private PassthroughSourceFactory(SourceT source) {
            this.source = source;
        }

        @Override
        public SourceT create(FromEnumT enumerator) {
            return source;
        }
    }

    /** Entry for list of underlying sources. */
    protected static class SourceListEntry implements Serializable {
        protected final SourceFactory configurer;
        protected final Boundedness boundedness;

        private SourceListEntry(SourceFactory configurer, Boundedness boundedness) {
            this.configurer = Preconditions.checkNotNull(configurer);
            this.boundedness = Preconditions.checkNotNull(boundedness);
        }

        public static SourceListEntry of(SourceFactory configurer, Boundedness boundedness) {
            return new SourceListEntry(configurer, boundedness);
        }
    }

    /** Builder for HybridSource. */
    public static class HybridSourceBuilder<T, EnumT extends SplitEnumerator>
            implements Serializable {
        private final List<SourceListEntry> sources;

        public HybridSourceBuilder() {
            sources = new ArrayList<>();
        }

        /** Add pre-configured source (without switch time modification). */
        public <ToEnumT extends SplitEnumerator, NextSourceT extends Source<T, ?, ?>>
                HybridSourceBuilder<T, ToEnumT> addSource(NextSourceT source) {
            return addSource(new PassthroughSourceFactory<>(source), source.getBoundedness());
        }

        /** Add source with deferred instantiation based on previous enumerator. */
        public <ToEnumT extends SplitEnumerator, NextSourceT extends Source<T, ?, ?>>
                HybridSourceBuilder<T, ToEnumT> addSource(
                        SourceFactory<T, NextSourceT, EnumT> sourceFactory,
                        Boundedness boundedness) {
            ClosureCleaner.clean(
                    sourceFactory, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            sources.add(SourceListEntry.of(sourceFactory, boundedness));
            return (HybridSourceBuilder) this;
        }

        /** Build the source. */
        public HybridSource<T> build() {
            return new HybridSource(sources);
        }
    }
}
