/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int waitTimeOut = 1000 * 5;
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<String, AllocateRequest>();
    //存放分配MappedFile的请求
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
        new PriorityBlockingQueue<AllocateRequest>();
    private volatile boolean hasException = false;
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     *
     * @param nextFilePath 下一个新的MappedFile的名称
     * @param nextNextFilePath 下下个新的 MappedFile 的名称
     * @param fileSize 每个 MappedFile 的大小
     * @return
     * 1、判断是否采用缓冲池：开启缓冲区池功能&&Master。如果开启缓冲池的功能，则根据池里剩余的缓冲buffer块来控制创建mappedFile的请求。
     *      如果采用缓冲池功能，且缓冲池的buffer块不够，则拒绝此次发起的创建MappedFile的请求，并直接返回。
     * 2、尝试向 ConcurrentHashMap 中存放 nextReq ，如果存放失败说明有别的线程已经创建文件，所以拒绝当前mappedFile的创建请求，并尝试另一个MappedFile的创建
     * 3、将待创建mappedFile的请求放入requestQueue队列。(该动作会触发AllocateMappedFileService本身从队列requestQueue出take出创建MappedFile的请求：AllocateRequest)
     * 4、当前线程超时等待AllocateRequest执行完成(AllocateMappedFileServic线程会不断的从equestQueue里获取弹出AllocateRequest进行执行，执行完成后会唤醒等待于当前AllocateReques的线程)
     * 5、MappedFile创建完成，则从requestTable移除该AllocateRequest请求。所以requestTable是用来维护未处理的AllocateRequest
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        int canSubmitRequests = 2;//默认可以提交2个请求
        //如果transientStorePoolEnable=true&&开启异步刷盘&&Master节点
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                //如果剩余的预分配的缓冲区跟requestQueue中待处理的分配MappedFile的数量
                canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs() - this.requestQueue.size();//总共剩余的数量 - 已经要分配的数量
            }
        }
        //创建添加nextNextFilePath的请求
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        // 尝试向 ConcurrentHashMap 中存放 nextReq ，如果存放失败说明有别的线程已经创建文件
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        if (nextPutOK) {//如果添加成功，意味着没有并发问题
            if (canSubmitRequests <= 0) {///如果剩余的预分配的缓冲区数量<=requestQueue中待处理的分配MappedFile的数量,则拒绝该次分配MappedFile的请求
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            //如果剩余的预分配的缓冲区数量>requestQueue中待处理的分配MappedFile的数量,则将该次分配MappedFile的请求加入队列requestQueue
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            canSubmitRequests--;
        }
        //同样的创建下下个MappedFile的请求
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {//成功的向requestQueue队列添加创建下一个MappedFile请求
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        if (hasException) {//mmapOperation遇到了异常，先不创建mappedFile了
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }
        //取出刚才创建的关于下一个文件的申请MappedFile的请求，每个AllocateRequest中都有一个CountDownLatch
        //AllocateMappedFileService本身是一个线程，会不挺的从requestQueue拉取AllocateRequest请求，执行完分配后会唤醒AllocateRequest中对应的CountDownLatch
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                //等待AllocateMappedFileService线程从requestQueue拉取AllocateRequest请求并执行完唤醒
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {//如果处理AllocateRequest创建MappedFile的请求成功，则从requestTable移除，所以这个requestTable存放的就是待分配MappedFile的AllocateRequest请求
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /***
     * 一个无限循环，判断当前线程的运行状态，并不断从requestQueue拉取分配MappedFile的AllocateRequest请求
     */
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 1、从队列requestQueue中弹出分配MappedFile的请求
     * 2、解析需要申请的MappedFile
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                //如果开启了transientStorePoolEnable，则从TransientStorePool中直接获取预分配的buffer
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    //否则直接在磁盘上创建一个MappedFile，并分配一个同等大小的mappedByteBuffer
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }
                //计算创建该mappedFile耗时
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    //判断requestQueue剩余的待分配的请求
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                        + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                    .getMapedFileSizeCommitLog()
                    &&
                    this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                        this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            return fileSize == other.fileSize;
        }
    }
}
