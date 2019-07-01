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

package org.apache.flink.table.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.dag.TransformationContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.submitter.ClusterSubmitter;
import org.apache.flink.streaming.submitter.JobSubmitter;
import org.apache.flink.streaming.submitter.LocalSubmitter;
import org.apache.flink.streaming.submitter.PlanSubmitter;
import org.apache.flink.table.delegation.Executor;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.StreamPlanner} supports.
 */
@Internal
public class StreamExecutor implements Executor {
	// TODO: refactor to decouple with StreamExecutionEnvironment, make table api self-contained
	private final StreamExecutionEnvironment executionEnvironment;

	private final TransformationContext transformationContext;

	private final JobSubmitter jobSubmitter;

	StreamExecutor(StreamExecutionEnvironment executionEnvironment) {
		this.executionEnvironment = executionEnvironment;
		transformationContext = executionEnvironment.getTransformationContext();

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (env instanceof ContextEnvironment) {
			jobSubmitter = new ClusterSubmitter((ContextEnvironment) env);
		} else if (env instanceof OptimizerPlanEnvironment || env instanceof PreviewPlanEnvironment) {
			jobSubmitter = new PlanSubmitter(env);
		} else {
			jobSubmitter = new LocalSubmitter(new Configuration());
		}
	}

	@Override
	public void apply(List<Transformation<?>> transformations) {
		transformationContext.addTransformations(transformations);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		StreamGraphGenerator generator = new StreamGraphGenerator(
				transformationContext.getTransformations(),
				executionEnvironment.getConfig(),
				executionEnvironment.getCheckpointConfig());

		// set customized properties of executor
		generator.setStateBackend(executionEnvironment.getStateBackend())
			.setChaining(executionEnvironment.isChainingEnabled())
			.setUserArtifacts(executionEnvironment.getCachedFiles())
			.setTimeCharacteristic(executionEnvironment.getStreamTimeCharacteristic())
			.setDefaultBufferTimeout(executionEnvironment.getBufferTimeout())
			.setJobName(jobName);

		StreamGraph streamGraph = generator.generate();

		return jobSubmitter.execute(streamGraph);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}
}
