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


package com.dtstack.flinkx.outputformat;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * @author jiangbo
 * @date 2019/8/28
 */
public abstract class BaseFileOutputFormat extends BaseRichOutputFormat {

    protected RowData lastRowData;

    protected String currentBlockFileNamePrefix;

    protected String currentBlockFileName;

    protected long sumRowsOfBlock;

    protected long rowsOfCurrentBlock;

    protected  long maxFileSize;

    protected long flushInterval = 0;

    protected static final String APPEND_MODE = "APPEND";

    protected static final String DATA_SUBDIR = ".data";

    protected static final String FINISHED_SUBDIR = ".finished";

    protected static final String ACTION_FINISHED = ".action_finished";

    protected static final String RESTART_FILE_NAME_SUFFIX = "restart";

    protected static final String JOB_ID_DELIMITER = "_";

    protected static final int SECOND_WAIT = 30;

    protected static final String SP = "/";

    protected String charsetName = "UTF-8";

    protected String outputFilePath;

    protected String path;

    protected String fileName;

    protected String tmpPath;

    protected String finishedPath;

    protected String actionFinishedTag;

    /** 写入模式 */
    protected String writeMode;

    /** 压缩方式 */
    protected String compress;

    protected boolean readyCheckpoint;

    protected int blockIndex = 0;

    protected boolean makeDir = true;

    private long nextNumForCheckDataSize = 1000;

    private long lastWriteSize;

    protected long lastWriteTime = System.currentTimeMillis();

    @Override
    public void initializeGlobal(int parallelism) {
        //TODO
        //1、创建.data目录或者删除.data目录中的数据
        //2、overwrite模式将原有数据移动至创建的.temData目录中
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        //TODO
        //1、将.data目录中的数据文件移动到正式目录中，删除.data目录
        //2、删除.temData目录
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        initFileIndex();
        initPath();
        openSource();
        actionBeforeWriteData();

        nextBlock();
    }

    protected void initPath(){
        if(StringUtils.isNotBlank(fileName)) {
            outputFilePath = path + SP + fileName;
        } else {
            outputFilePath = path;
        }

        currentBlockFileNamePrefix = taskNumber + "." + jobId;
        tmpPath = outputFilePath + SP + DATA_SUBDIR;
        finishedPath = outputFilePath + SP + FINISHED_SUBDIR + SP + taskNumber;
        actionFinishedTag = tmpPath + SP + ACTION_FINISHED + "_" + jobId;

        LOG.info("Channel:[{}], currentBlockFileNamePrefix:[{}], tmpPath:[{}], finishedPath:[{}]",
                taskNumber, currentBlockFileNamePrefix, tmpPath, finishedPath);
    }

    protected void initFileIndex() {
        if (null != formatState && formatState.getFileIndex() > -1) {
            blockIndex = formatState.getFileIndex() + 1;
        }

        LOG.info("Start block index:{}", blockIndex);
    }

    protected void actionBeforeWriteData(){
        if(taskNumber > 0){
            waitForActionFinishedBeforeWrite();
            return;
        }

        checkOutputDir();

        try{
            // 覆盖模式并且不是从检查点恢复时先删除数据目录
            boolean isCoverageData = !APPEND_MODE.equalsIgnoreCase(writeMode) && (formatState == null || formatState.getState() == null);
            if(isCoverageData){
                coverageData();
            }

            // 处理上次任务因异常失败产生的脏数据
            if (formatState != null) {
                cleanDirtyData();
            }
        } catch (Exception e){
            LOG.error("e = {}", ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }

        try {
            LOG.info("Delete [.data] dir before write records");
            clearTemporaryDataFiles();
        } catch (Exception e) {
            LOG.warn("Clean temp dir error before write records:{}", e.getMessage());
        } finally {
            createActionFinishedTag();
        }
    }

    @Override
    public void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
//        if (config.getRestore().isRestore() && !config.getRestore().isStream()){
//            if(lastRow != null){
//                readyCheckpoint = !ObjectUtils.equals(lastRow.getField(config.getRestore().getRestoreColumnIndex()),
//                        ((GenericRowData)rowData).getField(config.getRestore().getRestoreColumnIndex()));
//            }
//        }

        checkSize();

        writeSingleRecordToFile(rowData);

        lastWriteTime = System.currentTimeMillis();
    }

    private void checkSize() {
        if(numWriteCounter.getLocalValue() < nextNumForCheckDataSize){
            return;
        }

        if(getCurrentFileSize() > maxFileSize){
            try {
                flushData();
                LOG.info("Flush data by check file size");
            } catch (Exception e){
                throw new RuntimeException("Flush data error", e);
            }

            lastWriteSize = bytesWriteCounter.getLocalValue();
        }

        nextNumForCheckDataSize = getNextNumForCheckDataSize();
    }

    private long getCurrentFileSize(){
        return  (long)(getDeviation() * (bytesWriteCounter.getLocalValue() - lastWriteSize));
    }

    private long getNextNumForCheckDataSize(){
        long totalBytesWrite = bytesWriteCounter.getLocalValue();
        long totalRecordWrite = numWriteCounter.getLocalValue();

        float eachRecordSize = (totalBytesWrite * getDeviation()) / totalRecordWrite;

        long currentFileSize = getCurrentFileSize();
        long recordNum = (long)((maxFileSize - currentFileSize) / eachRecordSize);

        return totalRecordWrite + recordNum;
    }

    protected void nextBlock(){
//        if (config.getRestore().isRestore()){
//            currentBlockFileName = "." + currentBlockFileNamePrefix + "." + blockIndex + getExtension();
//        } else {
//            currentBlockFileName = currentBlockFileNamePrefix + "." + blockIndex + getExtension();
//        }
        currentBlockFileName = currentBlockFileNamePrefix + "." + blockIndex + getExtension();
    }

    @Override
    public FormatState getFormatState() throws Exception{
//        if (!config.getRestore().isRestore() || lastRow == null){
//            return null;
//        }

//        if (config.getRestore().isStream() || readyCheckpoint){
//            try{
//                flushData();
//                lastWriteSize = bytesWriteCounter.getLocalValue();
//            } catch (Exception e){
//                throw new RuntimeException("Flush data error when create snapshot:", e);
//            }
//
//            try{
//                if (sumRowsOfBlock != 0) {
//                    moveTemporaryDataFileToDirectory();
//                }
//            } catch (Exception e){
//                throw new RuntimeException("Move temporary file to data directory error when create snapshot:", e);
//            }
//
//            snapshotWriteCounter.add(sumRowsOfBlock);
//            numWriteCounter.add(sumRowsOfBlock);
//            formatState.setNumberWrite(numWriteCounter.getLocalValue());
//            if (!config.getRestore().isStream()){
//                formatState.setState(lastRow.getField(config.getRestore().getRestoreColumnIndex()));
//            }
//            sumRowsOfBlock = 0;
//            formatState.setJobId(jobId);
//            formatState.setFileIndex(blockIndex-1);
//            LOG.info("jobId = {}, blockIndex = {}", jobId, blockIndex);
//
//            super.getFormatState();
//            return formatState;
//        }

        //todo 通道文件压缩，标记需要移动到正式目录的数据文件，并将数据文件移动到正式目录

        return null;
    }

    /**
     * checkpoint成功时操作
     * @param checkpointId
     */
    public void notifyCheckpointComplete(long checkpointId){
        //todo 移动成功，清空标记
    };

    /**
     * checkpoint失败时操作
     * @param checkpointId
     */
    public void notifyCheckpointAborted(long checkpointId){
        //todo 根据标记检测是否已经有文件被移动到正式目录，若有，则移动回.data目录，然后清空标记
    };

    @Override
    public void closeInternal() throws IOException {
        readyCheckpoint = false;
        //最后触发一次 block文件重命名，为 .data 目录下的文件移动到数据目录做准备
        if(isTaskEndsNormally()){
            flushData();
            //restore == false 需要主动执行
//            if (!config.getRestore().isRestore()) {
//                moveTemporaryDataBlockFileToDirectory();
//            }
        }
        numWriteCounter.add(sumRowsOfBlock);
    }

//    @Override
    protected void afterCloseInternal()  {
        try {
            if(!isTaskEndsNormally()){
                return;
            }

//            if (!config.getRestore().isStream()) {
//                createFinishedTag();
//
//                if(taskNumber == 0) {
//                    waitForAllTasksToFinish();
//
//                    //正常被close，触发 .data 目录下的文件移动到数据目录
//                    moveAllTemporaryDataFileToDirectory();
//
//                    LOG.info("The task ran successfully,clear temporary data files");
//                    closeSource();
//                    clearTemporaryDataFiles();
//                }
//            }else{
//                closeSource();
//            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected boolean isTaskEndsNormally() throws IOException{
        String state = getTaskState();
        LOG.info("State of current task is:[{}]", state);
        if(!RUNNING_STATE.equals(state)){
//            if (!config.getRestore().isRestore()){
//                LOG.info("The task does not end normally, clear the temporary data file");
//                clearTemporaryDataFiles();
//            }

            closeSource();
            return false;
        }

        return true;
    }

    @Override
    public void tryCleanupOnError() throws Exception {
//        if(!config.getRestore().isRestore()) {
//            LOG.info("Clean temporary data in method tryCleanupOnError");
//            clearTemporaryDataFiles();
//        }
    }

    public String getPath() {
        return path;
    }

    public void flushData() throws IOException{
        if (rowsOfCurrentBlock != 0) {
            flushDataInternal();
//            if (config.getRestore().isRestore()) {
//                moveTemporaryDataBlockFileToDirectory();
//                sumRowsOfBlock += rowsOfCurrentBlock;
//                LOG.info("flush file:{} rows:{} sumRowsOfBlock:{}", currentBlockFileName, rowsOfCurrentBlock, sumRowsOfBlock);
//            }
            rowsOfCurrentBlock = 0;
        }
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    /**
     * 清除脏数据文件
     */
    protected abstract void cleanDirtyData();

    /**
     * 写数据前由第一个通道完成指定操作之后调用此方法创建结束标制通知其它通道开始写数据
     */
    protected abstract void createActionFinishedTag();

    /**
     * 等待第一个通道完成写数据前的操作
     */
    protected abstract void waitForActionFinishedBeforeWrite();

    /**
     * flush数据到存储介质
     *
     * @throws IOException 输出异常
     */
    protected abstract void flushDataInternal() throws IOException;

    /**
     * 单条数据写入文件
     *
     * @param rowData 要写入的数据
     * @throws WriteRecordException 脏数据异常
     */
    protected abstract void writeSingleRecordToFile(RowData rowData) throws WriteRecordException;

    /**
     * 每个通道写完数据后关闭资源前创建结束标制
     *
     * @throws IOException 创建异常
     */
    protected abstract void createFinishedTag() throws IOException;

    /**
     * 移动临时数据文件
     */
    protected abstract void moveTemporaryDataBlockFileToDirectory();

    /**
     * 等待所有通道操作完成
     *
     * @throws IOException 超时异常
     */
    protected abstract void waitForAllTasksToFinish() throws IOException;

    /**
     * 覆盖数据操作
     *
     * @throws IOException 删除数据异常
     */
    protected abstract void coverageData() throws IOException;

    /**
     * 移动所有的临时数据文件
     *
     * @throws IOException 重命名文件异常
     */
    protected abstract void moveTemporaryDataFileToDirectory() throws IOException;

    /**
     * 正常被close，触发 .data 目录下的文件移动到数据目录
     *
     * @throws IOException 重命名文件异常
     */
    protected abstract void moveAllTemporaryDataFileToDirectory() throws IOException;

    /**
     * 检查写入路径是否存在，是否为目录
     */
    protected abstract void checkOutputDir();

    /**
     * 打开资源
     *
     * @throws IOException 打开连接异常
     */
    protected abstract void openSource() throws IOException;

    /**
     * 关闭资源
     *
     * @throws IOException 关闭连接异常
     */
    protected abstract void closeSource() throws IOException;

    /**
     * 清除临时数据文件
     *
     * @throws IOException 删除数据异常
     */
    protected abstract void clearTemporaryDataFiles() throws IOException;

    /**
     * 获取文件压缩比
     * @return 压缩比 < 1
     */
    public abstract float getDeviation();

    /**
     * 获取文件后缀
     *
     * @return .gz
     */
    protected abstract String getExtension();
}
