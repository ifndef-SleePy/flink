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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface Job submitter.
 */
public class LocalSubmitter implements JobSubmitter {

	private static final Logger LOG = LoggerFactory.getLogger(LocalSubmitter.class);

	private final Configuration configuration;

	public LocalSubmitter(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {

		JobGraph jobGraph = streamGraph.getJobGraph();
		jobGraph.setAllowQueuedScheduling(true);

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());
		configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");

		// add (and override) the settings with what the user defined
		configuration.addAll(this.configuration);

		if (!configuration.contains(RestOptions.BIND_PORT)) {
			configuration.setString(RestOptions.BIND_PORT, "0");
		}

		int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

		MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
			.build();

		if (LOG.isInfoEnabled()) {
			LOG.info("Running job on local embedded Flink mini cluster");
		}

		try (MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();
			configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

			return miniCluster.executeJobBlocking(jobGraph);
		}
	}
}
