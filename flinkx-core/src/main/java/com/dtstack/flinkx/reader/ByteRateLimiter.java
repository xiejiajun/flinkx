/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.reader;

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This class is user for speed control
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ByteRateLimiter {

    private final static Logger LOG = LoggerFactory.getLogger(ByteRateLimiter.class);

    public static final int MIN_RECORD_NUMBER_UPDATE_RATE = 1000;

    private RateLimiter rateLimiter;

    /**
     * 所有subTask每秒允许处理的总Byte数
     * TODO 从任务json文件的job.setting.speed.bytes解析得出
     */
    private double expectedBytePerSecond;

    private AccumulatorCollector accumulatorCollector;

    private ScheduledExecutorService scheduledExecutorService;

    public ByteRateLimiter(AccumulatorCollector accumulatorCollector, double expectedBytePerSecond) {
        double initialRate = 1000.0;
        this.rateLimiter = RateLimiter.create(initialRate);
        this.expectedBytePerSecond = expectedBytePerSecond;
        this.accumulatorCollector = accumulatorCollector;

        ThreadFactory threadFactory = new BasicThreadFactory
                .Builder()
                .namingPattern("ByteRateCheckerThread-%d")
                .daemon(true)
                .build();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, threadFactory);
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(this::updateRate,0, 1000L, TimeUnit.MILLISECONDS);
    }

    public void stop(){
        if(scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    public void acquire() {
        rateLimiter.acquire();
    }

    private void updateRate(){
        // TODO 所有subTask读取到的总bytes数
        long totalBytes = accumulatorCollector.getAccumulatorValue(Metrics.READ_BYTES);
        // TODO 当前subTask读取到的记录数
        long thisRecords = accumulatorCollector.getLocalAccumulatorValue(Metrics.NUM_READS);
        // TODO 所有subTask读取到的总记录数
        long totalRecords = accumulatorCollector.getAccumulatorValue(Metrics.NUM_READS);

        // TODO 当前subTask的写出率 = 当前subTask读取的记录数 / 所有subTask读取的总记录数
        BigDecimal thisWriteRatio = BigDecimal.valueOf(totalRecords == 0 ? 0 : thisRecords / (double) totalRecords);

        // TODO 如果所有subTask读取的总记录数大于1000，并且总bytes数不为0, 并且当前subTask的写出率不为0
        //  (用于确保当前subTask已经读取并处理数据)，则调整当前subTask的限流策略
        if (totalRecords > MIN_RECORD_NUMBER_UPDATE_RATE && totalBytes != 0
                && thisWriteRatio.compareTo(BigDecimal.ZERO) != 0) {
            // TODO 计算每条记录的平均byte数
            double bpr = totalBytes / (double)totalRecords;
            // TODO 当前subTask限流器每秒允许处理的记录数 = json配置文件中设置的每秒允许处理的总byte数 * 当前subTask的处理比例 / 单条记录的平均byte数
            double permitsPerSecond = expectedBytePerSecond / bpr * thisWriteRatio.doubleValue();
            // TODO 按当前subTask处理数据的占比刷新当前subTask进程内限流器的限流速率
            rateLimiter.setRate(permitsPerSecond);
        }
    }
}
