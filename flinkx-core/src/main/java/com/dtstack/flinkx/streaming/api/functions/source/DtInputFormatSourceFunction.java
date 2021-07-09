/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.streaming.api.functions.source;

import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A {@link SourceFunction} that reads data using an {@link InputFormat}.
 *
 * @author jiangbo
 */
@Internal
public class DtInputFormatSourceFunction<OUT> extends InputFormatSourceFunction<OUT> implements CheckpointedFunction {
	private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DtInputFormatSourceFunction.class);

	private TypeInformation<OUT> typeInfo;
	private transient TypeSerializer<OUT> serializer;

	private InputFormat<OUT, InputSplit> format;

	private transient InputSplitProvider provider;
	private transient Iterator<InputSplit> splitIterator;

	private volatile boolean isRunning = true;

	/**
	 * 保存taskId -> Task State的映射关系
	 */
    private Map<Integer,FormatState> formatStateMap;

    private static final String LOCATION_STATE_NAME = "data-sync-location-states";

    private transient ListState<FormatState> unionOffsetStates;

    private boolean isStream;

	@SuppressWarnings("unchecked")
	public DtInputFormatSourceFunction(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
		super(format, typeInfo);
		this.format = (InputFormat<OUT, InputSplit>) format;
		this.typeInfo = typeInfo;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

		if (format instanceof RichInputFormat) {
			((RichInputFormat) format).setRuntimeContext(context);
		}

        if (format instanceof BaseRichInputFormat){
			RestoreConfig restoreConfig = ((BaseRichInputFormat) format).getRestoreConfig();
			isStream = restoreConfig != null && restoreConfig.isStream();
            if(formatStateMap != null){
                ((BaseRichInputFormat) format).setRestoreState(formatStateMap.get(context.getIndexOfThisSubtask()));
            }
        }

		format.configure(parameters);

		provider = context.getInputSplitProvider();
		serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
		splitIterator = getInputSplits();
		isRunning = splitIterator.hasNext();
	}

	/**
	 * TODO Operator中整个数据处理的流程都在这里，这里是重写了
	 *  org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction#run(SourceFunction.SourceContext)
	 * @param ctx
	 * @throws Exception
	 */
	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		Exception tryException = null;
		try {

			Counter completedSplitsCounter = getRuntimeContext().getMetricGroup().counter("numSplitsProcessed");
			if (isRunning && format instanceof RichInputFormat) {
				((RichInputFormat) format).openInputFormat();
			}

			OUT nextElement = serializer.createInstance();
			while (isRunning) {
				// TODO 调用的是BaseRichInputFormat中的open方法
				format.open(splitIterator.next());

				// for each element we also check if cancel
				// was called by checking the isRunning flag

				// TODO JdbcInputFormat/BaseHdfsInputFormat.reachedEnd等
				//   实时任务和离线任务的处理逻辑区别在于format.reachedEnd()，离线任务在数据结束后会返回true，停止发送事件等待Job退出
				//   实时任务format.reachedEnd()总是为false，会一致轮询等待新的数据到来，然后往下发
				while (isRunning && !format.reachedEnd()) {
				    if(isStream){
				    	// 实时任务
				    	// TODO BaseRichInputFormat.nextRecord: 处理记录
                        nextElement = format.nextRecord(nextElement);
                        if (nextElement != null) {
                        	// TODO 往下游算子发送数据
                            ctx.collect(nextElement);
                        }
                    } else {
				    	// 离线任务
                        synchronized (ctx.getCheckpointLock()){
							// TODO BaseRichInputFormat.nextRecord: 处理记录
                            nextElement = format.nextRecord(nextElement);
                            if (nextElement != null) {
								// TODO 往下游算子发送数据
                                ctx.collect(nextElement);
                            }
                        }
                    }
				}
				// TODO BaseRichInputFormat.close
				format.close();
				// TODO 基于计数器的numSplitsProcessed指标加1
				completedSplitsCounter.inc();

				if (isRunning) {
					// TODO 判断是否还有分片，
					isRunning = splitIterator.hasNext();
				}
			}
		} catch (Exception exception){
				tryException = exception;
		} finally {
			isRunning = false;
			try {
				format.close();
				if (format instanceof RichInputFormat) {
					((RichInputFormat) format).closeInputFormat();
				}
			}catch (Exception finallyException){
				if(null != tryException){
					LOG.error(ExceptionUtil.getErrorMessage(finallyException));
					tryException.addSuppressed(finallyException);
					throw tryException;
				}else {
					throw finallyException;
				}
			}
			if(null != tryException) {
				throw tryException;
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() throws Exception {
		format.close();
		if (format instanceof RichInputFormat) {
			((RichInputFormat) format).closeInputFormat();
		}
	}

	/**
	 * Returns the {@code InputFormat}. This is only needed because we need to set the input
	 * split assigner on the {@code StreamGraph}.
	 */
	@Override
	public InputFormat<OUT, InputSplit> getFormat() {
		return format;
	}

	private Iterator<InputSplit> getInputSplits() {

		return new Iterator<InputSplit>() {

			private InputSplit nextSplit;

			private boolean exhausted;

			@Override
			public boolean hasNext() {
				if (exhausted) {
					return false;
				}

				if (nextSplit != null) {
					return true;
				}

				final InputSplit split;
				try {
					split = provider.getNextInputSplit(getRuntimeContext().getUserCodeClassLoader());
				} catch (InputSplitProviderException e) {
					throw new RuntimeException("Could not retrieve next input split.", e);
				}

				if (split != null) {
					this.nextSplit = split;
					return true;
				} else {
					exhausted = true;
					return false;
				}
			}

			@Override
			public InputSplit next() {
				if (this.nextSplit == null && !hasNext()) {
					throw new NoSuchElementException();
				}

				final InputSplit tmp = this.nextSplit;
				this.nextSplit = null;
				return tmp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// TODO 获取自定义状态对象
        FormatState formatState = ((BaseRichInputFormat) format).getFormatState();
        if (formatState != null){
            LOG.info("InputFormat format state:{}", formatState.toString());
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	    LOG.info("Start initialize input format state");

		OperatorStateStore stateStore = context.getOperatorStateStore();
		// TODO 注册状态
        unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor<>(
				LOCATION_STATE_NAME,
				TypeInformation.of(new TypeHint<FormatState>() {})));

        LOG.info("Is restored:{}", context.isRestored());
		if (context.isRestored()){
			// TODO 如果是从CheckPoint恢复
			formatStateMap = new HashMap<>(16);
			for (FormatState formatState : unionOffsetStates.get()) {
				formatStateMap.put(formatState.getNumOfSubTask(), formatState);
				LOG.info("Input format state into:{}", formatState.toString());
			}
		}

        LOG.info("End initialize input format state");
	}
}
