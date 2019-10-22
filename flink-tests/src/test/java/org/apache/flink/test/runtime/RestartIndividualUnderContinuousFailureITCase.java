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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartIndividualStrategy;
import org.apache.flink.runtime.io.network.partition.ConsumptionDeclinedException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test handling of Producer failure when using restart individual strategy.
 */
public class RestartIndividualUnderContinuousFailureITCase extends TestLogger {

	private static int sourceCount = 2;
	private static int mapperCount = 2;
	private static int sinkCount = 2;

	@Rule
	public final MiniClusterResource miniClusterResource = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			getConfiguration(),
			3,
			2));

	private Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setString(EXECUTION_FAILOVER_STRATEGY.key(), TestingRestartIndividualStrategyFactory.class.getName());
		config.setInteger(MEMORY_SEGMENT_SIZE, 4096);
		config.setString(TaskManagerOptions.TASK_BEHAVIOR_ON_CONSUMER_FAILURE, "DRAIN");
		config.setLong(TaskManagerOptions.TASK_MANAGER_FAILED_INPUT_CHANNEL_UPDATING_TIMEOUT, 10000L);
		return config;
	}

	@Test(timeout = 120000)
	public void testContinuousFailure() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10000, 0));
		env.setBufferTimeout(1);

		// get input data
		final DataStream<Tuple2<String, Long>> source =
			env.addSource(new InfiniteSourceFunction()).setParallelism(sourceCount);
		// There should be at least one LocalInputChannel and RemoteInputChannel
		source
			.keyBy(0)
			.map(new TestMapFunction())
			.setParallelism(mapperCount)
			.keyBy(0)
			.addSink(new TestSinkFunction())
			.setParallelism(sinkCount);

		env.submit("testContinuousFailure");

		// wait a while
		while (TestSinkFunction.processed.get() < 100) {
			Thread.sleep(10);
		}

		// Fail the map task
		TestMapFunction.triggerFailure.set(true);

		while (TestMapFunction.attempt.get() < 100) {
			log.info("test map function attempt {}", TestMapFunction.attempt.get());
			Thread.sleep(100);
		}

		TestMapFunction.triggerFailure.set(false);
		InfiniteSourceFunction.running = false;

		Thread.sleep(1000);

		assertFalse(InfiniteSourceFunction.cancelled);
		assertEquals(sourceCount, InfiniteSourceFunction.attempt.get());
		assertEquals(sinkCount, TestSinkFunction.attempt.get());

		synchronized (TestingRestartIndividualStrategy.expectedFailureCauseCount) {
			log.info("Expected exceptions {}", TestingRestartIndividualStrategy.expectedFailureCauseCount);
		}

		synchronized (TestingRestartIndividualStrategy.unexpectedFailureCauseCount) {
			log.info("Unexpected exceptions {}", TestingRestartIndividualStrategy.unexpectedFailureCauseCount);
			assertTrue(TestingRestartIndividualStrategy.unexpectedFailureCauseCount.isEmpty());
		}
	}

	/**
	 * Test infinite source function.
	 */
	static class InfiniteSourceFunction extends RichParallelSourceFunction<Tuple2<String, Long>> {

		public static AtomicInteger attempt = new AtomicInteger(0);

		public static volatile boolean running = true;

		public static volatile boolean cancelled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			attempt.incrementAndGet();
		}

		@Override
		public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
			for (long l = 0; running && l < Long.MAX_VALUE; l++) {
				ctx.collect(Tuple2.of(Long.toString(l), l));
			}
		}

		@Override
		public void cancel() {
			running = false;
			cancelled = true;
		}
	}

	/**
	 * Test map function.
	 */
	static class TestMapFunction extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
		public static AtomicInteger attempt = new AtomicInteger(0);

		public static AtomicBoolean triggerFailure = new AtomicBoolean(false);

		private int processed = 0;

		private int failedAfterProcessing;

		@Override
		public void open(Configuration parameters) throws Exception {
			attempt.incrementAndGet();
			failedAfterProcessing = new Random().nextInt(100);
		}

		@Override
		public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
			if (triggerFailure.get() && processed++ == failedAfterProcessing) {
				throw new TestingException("Trigger failure of map task");
			}
			return Tuple2.of(value.f0 + "_mapped", value.f1);
		}
	}

	/**
	 * Test sink function.
	 */
	public static class TestSinkFunction extends RichSinkFunction<Tuple2<String, Long>> {

		public static AtomicInteger attempt = new AtomicInteger(0);

		public static AtomicInteger closed = new AtomicInteger(0);

		public static AtomicLong processed = new AtomicLong(0);

		@Override
		public void open(Configuration parameters) throws Exception {
			attempt.incrementAndGet();
		}

		@Override
		public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
			processed.incrementAndGet();
		}

		@Override
		public void close() throws Exception {
			closed.incrementAndGet();
		}
	}

	/**
	 * Testing failover strategy.
	 */
	static class TestingRestartIndividualStrategy extends RestartIndividualStrategy {

		private static final Logger LOG = LoggerFactory.getLogger(TestingRestartIndividualStrategy.class);

		public static Map<Class, AtomicLong> expectedFailureCauseCount = new HashMap<>();

		public static Map<Class, AtomicLong> unexpectedFailureCauseCount = new HashMap<>();

		public static AtomicInteger failedCount = new AtomicInteger(0);

		public TestingRestartIndividualStrategy(ExecutionGraph executionGraph, Executor callbackExecutor) {
			super(executionGraph, callbackExecutor);
		}

		@Override
		public void onTaskFailure(Execution taskExecution, Throwable cause) {
			failedCount.incrementAndGet();

			Class failureCauseClass = cause.getClass();
			boolean accepted = false;
			if (TestingException.class.equals(failureCauseClass)) {
				accepted = true;
			} else if (ConsumptionDeclinedException.class.equals(failureCauseClass)) {
				accepted = true;
			} else if (IOException.class.equals(failureCauseClass)) {
				if (ExceptionUtils.findThrowableWithMessage(cause, "Buffer pool is destroyed").isPresent() ||
					ExceptionUtils.findThrowable(cause, PartitionNotFoundException.class).isPresent()) {
					// See details in https://work.aone.alibaba-inc.com/issue/22265468
					// there might be a race condition of partition updating
					accepted = true;
				}
			}
			if (accepted) {
				synchronized (expectedFailureCauseCount) {
					expectedFailureCauseCount.putIfAbsent(failureCauseClass, new AtomicLong(0));
					expectedFailureCauseCount.get(failureCauseClass).incrementAndGet();
				}
			} else {
				LOG.warn("There is an unexpected failure cause", cause);
				synchronized (unexpectedFailureCauseCount) {
					unexpectedFailureCauseCount.putIfAbsent(failureCauseClass, new AtomicLong(0));
					unexpectedFailureCauseCount.get(failureCauseClass).incrementAndGet();
				}
			}

			super.onTaskFailure(taskExecution, cause);
		}
	}

	/**
	 * Testing failover strategy factory.
	 */
	public static class TestingRestartIndividualStrategyFactory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new TestingRestartIndividualStrategy(executionGraph, executionGraph.getFutureExecutor());
		}
	}

	static class TestingException extends RuntimeException {

		public TestingException(String message) {
			super(message);
		}
	}
}
