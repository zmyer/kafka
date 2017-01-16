/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.internals.GlobalStateStoreProvider;
import org.apache.kafka.streams.state.internals.QueryableStoreProvider;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.StreamThreadStateStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

/**
 * Kafka Streams allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * The computational logic can be specified either by using the {@link TopologyBuilder} class to define the a DAG topology of
 * {@link org.apache.kafka.streams.processor.Processor}s or by using the {@link org.apache.kafka.streams.kstream.KStreamBuilder}
 * class which provides the high-level {@link org.apache.kafka.streams.kstream.KStream} DSL to define the transformation.
 * <p>
 * The {@link KafkaStreams} class manages the lifecycle of a Kafka Streams instance. One stream instance can contain one or
 * more threads specified in the configs for the processing work.
 * <p>
 * A {@link KafkaStreams} instance can co-ordinate with any other instances with the same application ID (whether in this same process, on other processes
 * on this machine, or on remote machines) as a single (possibly distributed) stream processing client. These instances will divide up the work
 * based on the assignment of the input topic partitions so that all partitions are being
 * consumed. If instances are added or failed, all instances will rebalance the partition assignment among themselves
 * to balance processing load.
 * <p>
 * Internally the {@link KafkaStreams} instance contains a normal {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}
 * and {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer} instance that is used for reading input and writing output.
 * <p>
 * <p>
 * A simple example might look like this:
 * <pre>
 *    Map&lt;String, Object&gt; props = new HashMap&lt;&gt;();
 *    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
 *    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 *    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 *    StreamsConfig config = new StreamsConfig(props);
 *
 *    KStreamBuilder builder = new KStreamBuilder();
 *    builder.stream("my-input-topic").mapValues(value -&gt; value.length().toString()).to("my-output-topic");
 *
 *    KafkaStreams streams = new KafkaStreams(builder, config);
 *    streams.start();
 * </pre>
 */

@InterfaceStability.Unstable
public class KafkaStreams {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreams.class);
    private static final String JMX_PREFIX = "kafka.streams";
    private static final int DEFAULT_CLOSE_TIMEOUT = 0;
    private GlobalStreamThread globalStreamThread;

    private final StreamThread[] threads;
    private final Map<Long, StreamThread.State> threadState;
    private final Metrics metrics;
    private final QueryableStoreProvider queryableStoreProvider;

    // processId is expected to be unique across JVMs and to be used
    // in userData of the subscription request to allow assignor be aware
    // of the co-location of stream thread's consumers. It is for internal
    // usage only and should not be exposed to users at all.
    private final UUID processId;
    private final StreamsMetadataState streamsMetadataState;

    private final StreamsConfig config;

    // container states
    /**
     * Kafka Streams states are the possible state that a Kafka Streams instance can be in.
     * An instance must only be in one state at a time.
     * Note this instance will be in "Rebalancing" state if any of its threads is rebalancing
     * The expected state transition with the following defined states is:
     *
     *                 +-----------+
     *         +<------|Created    |
     *         |       +-----+-----+
     *         |             |   +--+
     *         |             v   |  |
     *         |       +-----+---v--+--+
     *         +<----- | Rebalancing   |<--------+
     *         |       +-----+---------+         ^
     *         |                 +--+            |
     *         |                 |  |            |
     *         |       +-----+---v--+-----+      |
     *         +------>|Running           |------+
     *         |       +-----+------------+
     *         |             |
     *         |             v
     *         |     +-------+--------+
     *         +---->|Pending         |
     *               |Shutdown        |
     *               +-------+--------+
     *                       |
     *                       v
     *                 +-----+-----+
     *                 |Not Running|
     *                 +-----------+
     */
    public enum State {
        CREATED(1, 2, 3), RUNNING(2, 3), REBALANCING(1, 2, 3), PENDING_SHUTDOWN(4), NOT_RUNNING;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer...validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return this.equals(RUNNING) || this.equals(REBALANCING);
        }
        public boolean isCreatedOrRunning() {
            return isRunning() || this.equals(CREATED);
        }
        public boolean isValidTransition(final State newState) {
            return validTransitions.contains(newState.ordinal());
        }
    }
    private volatile State state = KafkaStreams.State.CREATED;
    private StateListener stateListener = null;
    private final StreamStateListener streamStateListener = new StreamStateListener();

    /**
     * Listen to state change events
     */
    public interface StateListener {

        /**
         * Called when state changes
         * @param newState     current state
         * @param oldState     previous state
         */
        void onChange(final State newState, final State oldState);
    }

    /**
     * An app can set {@link StateListener} so that the app is notified when state changes
     * @param listener
     */
    public void setStateListener(final StateListener listener) {
        this.stateListener = listener;
    }

    private synchronized void setState(State newState) {
        State oldState = state;
        if (!state.isValidTransition(newState)) {
            log.warn("Unexpected state transition from " + state + " to " + newState);
        }
        state = newState;
        if (stateListener != null) {
            stateListener.onChange(state, oldState);
        }
    }


    /**
     * @return The state this instance is in
     */
    public synchronized State state() {
        return state;
    }

    /**
     * Get read-only handle on global metrics registry
     * @return Map of all metrics.
     */
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    private class StreamStateListener implements StreamThread.StateListener {
        @Override
        public synchronized void onChange(final StreamThread thread, final StreamThread.State newState, final StreamThread.State oldState) {
            threadState.put(thread.getId(), newState);
            if (newState == StreamThread.State.PARTITIONS_REVOKED ||
                newState == StreamThread.State.ASSIGNING_PARTITIONS) {
                setState(State.REBALANCING);
            } else if (newState == StreamThread.State.RUNNING) {
                for (StreamThread.State state : threadState.values()) {
                    if (state != StreamThread.State.RUNNING) {
                        return;
                    }
                }
                setState(State.RUNNING);
            }
        }
    }
    /**
     * Construct the stream instance.
     *
     * @param builder the processor topology builder specifying the computational logic
     * @param props   properties for the {@link StreamsConfig}
     */
    public KafkaStreams(final TopologyBuilder builder, final Properties props) {
        this(builder, new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    /**
     * Construct the stream instance.
     *
     * @param builder the processor topology builder specifying the computational logic
     * @param config  the stream configs
     */
    public KafkaStreams(final TopologyBuilder builder, final StreamsConfig config) {
        this(builder, config, new DefaultKafkaClientSupplier());
    }

    /**
     * Construct the stream instance.
     *
     * @param builder        the processor topology builder specifying the computational logic
     * @param config         the stream configs
     * @param clientSupplier the kafka clients supplier which provides underlying producer and consumer clients
     *                       for this {@link KafkaStreams} instance
     */
    public KafkaStreams(final TopologyBuilder builder, final StreamsConfig config, final KafkaClientSupplier clientSupplier) {
        // create the metrics
        final Time time = Time.SYSTEM;

        processId = UUID.randomUUID();

        this.config = config;

        // The application ID is a required config and hence should always have value
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);

        builder.setApplicationId(applicationId);

        String clientId = config.getString(StreamsConfig.CLIENT_ID_CONFIG);
        if (clientId.length() <= 0)
            clientId = applicationId + "-" + processId;

        final List<MetricsReporter> reporters = config.getConfiguredInstances(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
            MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));

        final MetricConfig metricConfig = new MetricConfig().samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            .timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                TimeUnit.MILLISECONDS);

        metrics = new Metrics(metricConfig, reporters, time);

        threads = new StreamThread[config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG)];
        threadState = new HashMap<>(threads.length);
        final ArrayList<StateStoreProvider> storeProviders = new ArrayList<>();
        streamsMetadataState = new StreamsMetadataState(builder, parseHostInfo(config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG)));

        final ProcessorTopology globalTaskTopology = builder.buildGlobalStateTopology();

        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) < 0) {
            log.warn("Negative cache size passed in. Reverting to cache size of 0 bytes.");
        }

        final long cacheSizeBytes = Math.max(0, config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) /
                (config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG) + (globalTaskTopology == null ? 0 : 1)));


        if (globalTaskTopology != null) {
            globalStreamThread = new GlobalStreamThread(globalTaskTopology,
                                                        config,
                                                        clientSupplier.getRestoreConsumer(config.getRestoreConsumerConfigs(clientId + "-global")),
                                                        new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG)),
                                                        metrics,
                                                        time,
                                                        clientId);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new StreamThread(builder,
                                          config,
                                          clientSupplier,
                                          applicationId,
                                          clientId,
                                          processId,
                                          metrics,
                                          time,
                                          streamsMetadataState,
                                          cacheSizeBytes);
            threads[i].setStateListener(streamStateListener);
            threadState.put(threads[i].getId(), threads[i].state());
            storeProviders.add(new StreamThreadStateStoreProvider(threads[i]));
        }
        final GlobalStateStoreProvider globalStateStoreProvider = new GlobalStateStoreProvider(builder.globalStateStores());
        queryableStoreProvider = new QueryableStoreProvider(storeProviders, globalStateStoreProvider);
    }

    private static HostInfo parseHostInfo(final String endPoint) {
        if (endPoint == null || endPoint.trim().isEmpty()) {
            return StreamsMetadataState.UNKNOWN_HOST;
        }
        final String host = getHost(endPoint);
        final Integer port = getPort(endPoint);

        if (host == null || port == null) {
            throw new ConfigException(String.format("Error parsing host address %s. Expected format host:port.", endPoint));
        }

        return new HostInfo(host, port);
    }


    /**
     * Start the stream instance by starting all its threads.
     *
     * @throws IllegalStateException if process was already started
     */
    public synchronized void start() {
        log.debug("Starting Kafka Stream process");

        if (state == KafkaStreams.State.CREATED) {
            setState(KafkaStreams.State.RUNNING);

            if (globalStreamThread != null) {
                globalStreamThread.start();
            }

            for (final StreamThread thread : threads) {
                thread.start();
            }

            log.info("Started Kafka Stream process");
        } else {
            throw new IllegalStateException("Cannot start again.");
        }
    }

    /**
     * Shutdown this stream instance by signaling all the threads to stop,
     * and then wait for them to join.
     *
     * This will block until all threads have stopped.
     */
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT, TimeUnit.SECONDS);
    }

    /**
     * Shutdown this stream instance by signaling all the threads to stop,
     * and then wait up to the timeout for the threads to join.
     *
     * A timeout of 0 means to wait forever
     *
     * @param timeout   how long to wait for {@link StreamThread}s to shutdown
     * @param timeUnit  unit of time used for timeout
     * @return true if all threads were successfully stopped
     */
    public synchronized boolean close(final long timeout, final TimeUnit timeUnit) {
        log.debug("Stopping Kafka Stream process");
        if (state.isCreatedOrRunning()) {
            setState(KafkaStreams.State.PENDING_SHUTDOWN);
            // save the current thread so that if it is a stream thread
            // we don't attempt to join it and cause a deadlock
            final Thread shutdown = new Thread(new Runnable() {
                @Override
                public void run() {
                        // signal the threads to stop and wait
                        for (final StreamThread thread : threads) {
                            // avoid deadlocks by stopping any further state reports
                            // from the thread since we're shutting down
                            thread.setStateListener(null);
                            thread.close();
                        }
                        if (globalStreamThread != null) {
                            globalStreamThread.close();
                            if (!globalStreamThread.stillRunning()) {
                                try {
                                    globalStreamThread.join();
                                } catch (InterruptedException e) {
                                    Thread.interrupted();
                                }
                            }
                        }
                        for (final StreamThread thread : threads) {
                            try {
                                if (!thread.stillRunning()) {
                                    thread.join();
                                }
                            } catch (final InterruptedException ex) {
                                Thread.interrupted();
                            }
                        }

                        metrics.close();
                        log.info("Stopped Kafka Streams process");
                    }
            }, "kafka-streams-close-thread");
            shutdown.setDaemon(true);
            shutdown.start();
            try {
                shutdown.join(TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            setState(KafkaStreams.State.NOT_RUNNING);
            return !shutdown.isAlive();
        }
        return true;
    }

    /**
     * Produces a string representation containing useful information about Kafka Streams
     * Such as thread IDs, task IDs and a representation of the topology. This is useful
     * in debugging scenarios.
     * @return A string representation of the Kafka Streams instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about Kafka Streams
     * such as thread IDs, task IDs and a representation of the topology starting with the given indent. This is useful
     * in debugging scenarios.
     * @return A string representation of the Kafka Streams instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(indent + "KafkaStreams processID:" + processId + "\n");
        for (final StreamThread thread : threads) {
            sb.append(thread.toString(indent + "\t"));
        }
        sb.append("\n");

        return sb.toString();
    }

    /**
     * Cleans up local state store directory ({@code state.dir}), by deleting all data with regard to the application-id.
     * <p>
     * May only be called either before instance is started or after instance is closed.
     *
     * @throws IllegalStateException if instance is currently running
     */
    public void cleanUp() {
        if (state.isRunning()) {
            throw new IllegalStateException("Cannot clean up while running.");
        }

        final String appId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final String stateDir = config.getString(StreamsConfig.STATE_DIR_CONFIG);

        final String localApplicationDir = stateDir + File.separator + appId;
        log.debug("Removing local Kafka Streams application data in {} for application {}",
            localApplicationDir,
            appId);

        final StateDirectory stateDirectory = new StateDirectory(appId, stateDir);
        stateDirectory.cleanRemovedTasks();
    }

    /**
     * Sets the handler invoked when a stream thread abruptly terminates due to an uncaught exception.
     *
     * @param eh the object to use as this thread's uncaught exception handler. If null then this thread has no explicit handler.
     */
    public void setUncaughtExceptionHandler(final Thread.UncaughtExceptionHandler eh) {
        for (final StreamThread thread : threads) {
            thread.setUncaughtExceptionHandler(eh);
        }

        if (globalStreamThread != null) {
            globalStreamThread.setUncaughtExceptionHandler(eh);
        }
    }


    /**
     * Find all of the instances of {@link StreamsMetadata} in the {@link KafkaStreams} application that this instance belongs to
     *
     * Note: this is a point in time view and it may change due to partition reassignment.
     * @return collection containing all instances of {@link StreamsMetadata} in the {@link KafkaStreams} application that this instance belongs to
     */
    public Collection<StreamsMetadata> allMetadata() {
        validateIsRunning();
        return streamsMetadataState.getAllMetadata();
    }


    /**
     * Find instances of {@link StreamsMetadata} that contains the given storeName
     *
     * Note: this is a point in time view and it may change due to partition reassignment.
     * @param storeName the storeName to find metadata for
     * @return  A collection containing instances of {@link StreamsMetadata} that have the provided storeName
     */
    public Collection<StreamsMetadata> allMetadataForStore(final String storeName) {
        validateIsRunning();
        return streamsMetadataState.getAllMetadataForStore(storeName);
    }

    /**
     * Find the {@link StreamsMetadata} instance that contains the given storeName
     * and the corresponding hosted store instance contains the given key. This will use
     * the {@link org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner} to
     * locate the partition. If a custom partitioner has been used please use
     * {@link KafkaStreams#metadataForKey(String, Object, StreamPartitioner)}
     *
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which host it would exist on.
     *
     * Note: this is a point in time view and it may change due to partition reassignment.
     * @param storeName         Name of the store
     * @param key               Key to use to for partition
     * @param keySerializer     Serializer for the key
     * @param <K>               key type
     * @return  The {@link StreamsMetadata} for the storeName and key or {@link StreamsMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing
     */
    public <K> StreamsMetadata metadataForKey(final String storeName,
                                              final K key,
                                              final Serializer<K> keySerializer) {
        validateIsRunning();
        return streamsMetadataState.getMetadataWithKey(storeName, key, keySerializer);
    }

    /**
     * Find the {@link StreamsMetadata} instance that contains the given storeName
     * and the corresponding hosted store instance contains the given key
     *
     * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.StateStore},
     * this method provides a way of finding which host it would exist on.
     *
     * Note: this is a point in time view and it may change due to partition reassignment.
     * @param storeName         Name of the store
     * @param key               Key to use to for partition
     * @param partitioner       Partitioner for the store
     * @param <K>               key type
     * @return  The {@link StreamsMetadata} for the storeName and key or {@link StreamsMetadata#NOT_AVAILABLE}
     * if streams is (re-)initializing
     */
    public <K> StreamsMetadata metadataForKey(final String storeName,
                                              final K key,
                                              final StreamPartitioner<? super K, ?> partitioner) {
        validateIsRunning();
        return streamsMetadataState.getMetadataWithKey(storeName, key, partitioner);
    }


    /**
     * Get a facade wrapping the {@link org.apache.kafka.streams.processor.StateStore} instances
     * with the provided storeName and accepted by {@link QueryableStoreType#accepts(StateStore)}.
     * The returned object can be used to query the {@link org.apache.kafka.streams.processor.StateStore} instances
     * @param storeName             name of the store to find
     * @param queryableStoreType    accept only stores that are accepted by {@link QueryableStoreType#accepts(StateStore)}
     * @param <T>                   return type
     * @return  A facade wrapping the {@link org.apache.kafka.streams.processor.StateStore} instances
     * @throws org.apache.kafka.streams.errors.InvalidStateStoreException if the streams are (re-)initializing or
     * a store with storeName and queryableStoreType doesnt' exist.
     */
    public <T> T store(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        validateIsRunning();
        return queryableStoreProvider.getStore(storeName, queryableStoreType);
    }

    private void validateIsRunning() {
        if (!state.isRunning()) {
            throw new IllegalStateException("KafkaStreams is not running. State is " + state);
        }
    }
}
