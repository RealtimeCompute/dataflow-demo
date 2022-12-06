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

package com.alibaba.ververica.dataflow.demo.oss;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.dataflow.demo.oss.event.Event;
import com.alibaba.ververica.dataflow.demo.oss.event.EventDeSerializationSchema;

public class OssDemoJob {
    public static String OUTPUT_OSS_DIR = "outputOssDir";
    public static final String KAFKA_BROKERS_ARG = "kafkaBrokers";
    public static final String INPUT_TOPIC_ARG = "inputTopic";
    public static final String INPUT_TOPIC_GROUP_ARG = "inputTopicGroup";

    public static void checkArg(String argName, MultipleParameterTool params) {
        if (!params.has(argName)) {
            throw new IllegalArgumentException(argName + " must be set!");
        }
    }

    public static void main(String[] args) throws Exception {
        // Process args
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        checkArg(OUTPUT_OSS_DIR, params);
        checkArg(KAFKA_BROKERS_ARG, params);
        checkArg(INPUT_TOPIC_ARG, params);
        checkArg(INPUT_TOPIC_GROUP_ARG, params);

        // Check output oss dir
        Preconditions.checkArgument(
                params.get(OUTPUT_OSS_DIR).startsWith("oss://"),
                "outputOssDir should start with 'oss://'.");

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Checkpoint is required
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        String outputPath = params.get(OUTPUT_OSS_DIR);

        // Build Kafka source with new Source API based on FLIP-27
        KafkaSource<Event> kafkaSource =
                KafkaSource.<Event>builder()
                        .setBootstrapServers(params.get(KAFKA_BROKERS_ARG))
                        .setTopics(params.get(INPUT_TOPIC_ARG))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId(params.get(INPUT_TOPIC_GROUP_ARG))
                        .setDeserializer(new EventDeSerializationSchema())
                        .build();
        // DataStream Source
        DataStreamSource<Event> source =
                env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.getEventTime()),
                        "Kafka Source");

        StreamingFileSink<Event> sink =
                StreamingFileSink.forRowFormat(
                                new Path(outputPath), new SimpleStringEncoder<Event>("UTF-8"))
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .build();
        source.addSink(sink);

        // Compile and submit the job
        env.execute();

        //         flink run -t yarn-per-job -d dataflow-oss-demo-1.0-SNAPSHOT.jar  --outputOssDir
        //         oss://<YOUR_TARGET_BUCKET>/oss_kafka_test --kafkaBrokers core-1-1:9092
        // --inputTopic
        //         kafka-test-topic --inputTopicGroup my-group
    }
}
