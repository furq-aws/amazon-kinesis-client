/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
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

package software.amazon.kinesis.multilang.config;

import lombok.Getter;
import lombok.Setter;

import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;

@Getter
@Setter
public class WorkerMetricsTableConfigBean {

    interface WorkerMetricsTableConfigBeanDelegate {
        String getWorkerMetricsTableName();
        void setWorkerMetricsTableName(String value);

        long getReadCapacity();
        void setReadCapacity(long value);

        long getWriteCapacity();
        void setWriteCapacity(long value);
    }

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class, methodName = "tableName")
    private String workerMetricsTableName;

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class)
    private long readCapacity;

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class)
    private long writeCapacity;

    public WorkerMetricsTableConfig create(String applicationName) {
        return ConfigurationSettableUtils.resolveFields(
                this, new WorkerMetricsTableConfig(applicationName)
        );
    }
}
