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

package com.dtstack.flinkx.writer;

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Error Limitation
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ErrorLimiter {

    private final Integer maxErrors;
    private final Double maxErrorRatio;
    private AccumulatorCollector accumulatorCollector;
    private volatile double errorRatio = 0.0;
    private String errMsg = "";
    private Row errorData;

    public void setErrorData(Row errorData){
        this.errorData = errorData;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public ErrorLimiter(AccumulatorCollector accumulatorCollector, Integer maxErrors, Double maxErrorRatio) {
        this.maxErrors = maxErrors;
        this.maxErrorRatio = maxErrorRatio;
        this.accumulatorCollector = accumulatorCollector;
    }

    public void updateErrorInfo(){
        accumulatorCollector.collectAccumulator();
    }

    public void acquire() {
        String errorDataStr = "";
        if(errorData != null){
            errorDataStr = errorData.toString() + "\n";
        }

        // TODO 获取当前错误数全局值，全局值在AccumulatorCollector.start时又一个线程定时刷新
        long errors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS);
        if(maxErrors != null){
            // TODO 当前错误数errors大于最大允许的错误数maxErrors则抛出异常终止Job
            Preconditions.checkArgument(errors <= maxErrors, "WritingRecordError: error writing record [" + errors + "] exceed limit [" + maxErrors
                    + "]\n" + errorDataStr + errMsg);
        }

        if(maxErrorRatio != null){
            long numRead = accumulatorCollector.getAccumulatorValue(Metrics.NUM_READS);
            if(numRead >= 1) {
                // TODO  错误率 = 错误数 / 当前读取过的总记录数
                errorRatio = (double) errors / numRead;
            }

            // TODO 错误率当前errorRatio大于最大允许错误率maxErrorRatio则抛出异常终止Job
            Preconditions.checkArgument(errorRatio <= maxErrorRatio, "WritingRecordError: error writing record ratio [" + errorRatio + "] exceed limit [" + maxErrorRatio
                    + "]\n" + errorDataStr + errMsg);
        }
    }

}
