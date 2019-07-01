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

package org.apache.flink.streaming.submitter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface Job submitter.
 */
public class PlanSubmitter implements JobSubmitter {

	private static final Logger LOG = LoggerFactory.getLogger(PlanSubmitter.class);

	private final ExecutionEnvironment env;

	public PlanSubmitter(ExecutionEnvironment env) {
		this.env = env;
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {

		if (env instanceof OptimizerPlanEnvironment) {
			((OptimizerPlanEnvironment) env).setPlan(streamGraph);
		} else if (env instanceof PreviewPlanEnvironment) {
			((PreviewPlanEnvironment) env).setPreview(streamGraph.getStreamingPlanAsJSON());
		}

		throw new OptimizerPlanEnvironment.ProgramAbortException();
	}
}

