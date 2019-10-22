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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;
import static org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader.INDIVIDUAL_RESTART_STRATEGY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test handling of Producer failure when using restart individual strategy.
 */
public class RestartIndividualOfMiddleTaskFailureITCase extends TestLogger {

	@Rule
	public final MiniClusterResource miniClusterResource = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			getConfiguration(),
			2,
			2));

	private Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setString(EXECUTION_FAILOVER_STRATEGY.key(), INDIVIDUAL_RESTART_STRATEGY_NAME);
		config.setInteger(MEMORY_SEGMENT_SIZE, 4096);
		return config;
	}

	@Test(timeout = 30000)
	public void testMiddleTaskFailure() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10000, 1));

		// get input data
		final DataStream<Tuple2<String, Integer>> source = env.addSource(new InfiniteSourceFunction()).setParallelism(1);
		// There should be at least one LocalInputChannel and RemoteInputChannel
		source.keyBy(0).map(new TestMapFunction()).setParallelism(1).keyBy(0).addSink(new TestSinkFunction()).setParallelism(1);

		env.submit("testMiddleTaskFailure");

		// wait for output
		while (TestSinkFunction.processed == 0) {
			Thread.sleep(10);
		}

		// Fail the map task
		TestMapFunction.triggerFailure.set(true);

		// Wait for new source attempt restarting
		while (TestMapFunction.attempt.get() < 2) {
			Thread.sleep(10);
		}

		// Wait for the new records coming
		int processed = TestSinkFunction.processed;
		while (TestSinkFunction.processed == processed) {
			assertFalse(TestSinkFunction.failed);
			Thread.sleep(10);
		}

		Thread.sleep(100);

		InfiniteSourceFunction.running = false;

		// Wait job finishing
		while (TestSinkFunction.closed) {
			Thread.sleep(10);
		}

		assertFalse(TestSinkFunction.failed);
		assertEquals(1, TestSinkFunction.attempt.get());
		assertEquals(2, TestMapFunction.attempt.get());
		assertEquals(1, InfiniteSourceFunction.attempt.get());

		log.info("Running time {}, Failure time {}, Failure/Running {}",
			TestMapFunction.runningTime,
			TestMapFunction.failureTime,
			1.0 * TestMapFunction.failureTime / TestMapFunction.runningTime);

		log.info("Lost record {}, processed record {}, lost/processed {}",
			TestSinkFunction.lost,
			TestSinkFunction.processed,
			1.0 * TestSinkFunction.lost / TestSinkFunction.processed);
	}

	/**
	 * Test infinite source function.
	 */
	public static class InfiniteSourceFunction extends RichSourceFunction<Tuple2<String, Integer>> {

		public static AtomicInteger attempt = new AtomicInteger(0);

		public static AtomicBoolean triggerFailure = new AtomicBoolean(false);

		public static volatile boolean running = true;

		@Override
		public void open(Configuration parameters) throws Exception {
			attempt.incrementAndGet();
		}

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
			for (int i = 0; running && i < Integer.MAX_VALUE; i++) {
				if (triggerFailure.compareAndSet(true, false)) {
					throw new RuntimeException("Trigger failure");
				}
				ctx.collect(Tuple2.of(Integer.toString(i), i));
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	/**
	 * Test map function.
	 */
	public static class TestMapFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
		public static AtomicInteger attempt = new AtomicInteger(0);

		public static AtomicBoolean triggerFailure = new AtomicBoolean(false);

		public static volatile long runningTime = 0L;

		public static volatile long failureTime = 0L;

		private static volatile long openTimestamp = 0L;

		private static volatile long closeTimestamp = 0L;

		@Override
		public void open(Configuration parameters) throws Exception {
			if (attempt.get() > 100) {
				Thread.sleep(1000);
			}
			attempt.incrementAndGet();

			openTimestamp = System.currentTimeMillis();
			if (closeTimestamp != 0) {
				failureTime += openTimestamp - closeTimestamp;
			}
		}

		@Override
		public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
			if (triggerFailure.compareAndSet(true, false)) {
				throw new RuntimeException("Trigger failure of map task");
			}
			return Tuple2.of(value.f0 + "_mapped", value.f1);
		}

		@Override
		public void close() {
			closeTimestamp = System.currentTimeMillis();
			runningTime += closeTimestamp - openTimestamp;
		}
	}

	/**
	 * Test sink function.
	 */
	public static class TestSinkFunction extends RichSinkFunction<Tuple2<String, Integer>> {

		public static AtomicInteger attempt = new AtomicInteger(0);

		public static volatile int processed = 0;

		public static volatile int lost = 0;

		public static volatile int lastUpdated = 0;

		public static volatile boolean failed = false;

		public static volatile boolean closed = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			attempt.incrementAndGet();
		}

		@Override
		public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
			if (value.f1 != 0 && value.f1 != lastUpdated + 1) {
				if (value.f1 < lastUpdated) {
					failed = true;
				}
				lost += value.f1 - lastUpdated + 1;
			}
			lastUpdated = value.f1;
			processed++;
		}

		@Override
		public void close() {

		}
	}
}
