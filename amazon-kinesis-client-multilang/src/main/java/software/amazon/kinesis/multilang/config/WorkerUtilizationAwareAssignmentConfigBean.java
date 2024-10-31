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

import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
//import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig;

@Getter
@Setter
public class WorkerUtilizationAwareAssignmentConfigBean {

    interface WorkerUtilizationAwareAssignmentConfigBeanDelegate {
        long inMemoryWorkerMetricsCaptureFrequencyMillis = Duration.ofSeconds(1L).toMillis();

        void setInMemoryWorkerMetricsCaptureFrequencyMillis(long value);

    }

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class, convertToOptional = true)
    private long inMemoryWorkerMetricsCaptureFrequencyMillis;

    public WorkerUtilizationAwareAssignmentConfig create() {
        WorkerUtilizationAwareAssignmentConfig conf = new WorkerUtilizationAwareAssignmentConfig();
        conf.inMemoryWorkerMetricsCaptureFrequencyMillis(this.inMemoryWorkerMetricsCaptureFrequencyMillis);
        return conf;
    }
}
