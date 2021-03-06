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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    //操作系统每页大小，默认4K
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //当前JVM实例中MappedFile虚拟内存大小
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    //当前JVM实例中MappedFile对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    //当前该文件的写指针，从0开始，内存映射的写指针
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //已提交的指针
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    //已刷到磁盘的指针
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    //文件大小
    protected int fileSize;
    //文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    //堆内存byteBuffer，它和MappedFile的buffer不是同一个,如果不为空，数据首先会先存到该buffer中，然后提交到MappedFile映射的内存buffer中，transientStorePoolEnable为true时不为空
    protected ByteBuffer writeBuffer = null;
    //堆内存池，用来提供一种内存锁定，将当前堆外内存锁定，避免被进程将内存交换到磁盘
    protected TransientStorePool transientStorePool = null;
    private String fileName;//文件名称
    private long fileFromOffset;//该文件的初拾偏移量
    private File file;//物理文件
    private MappedByteBuffer mappedByteBuffer;//物理文件对应的内存映射buffer
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        //如果是堆外内存，则直接调用堆外内存的cleanup进行清理
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    /***
     * 反射访问方法
     * @param target
     * @param methodName
     * @param args
     * @return
     */
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    /***
     * 获得总的MappedFiles个数
     * @return
     */
    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    /***
     * 获得总的虚拟内存大小
     *
     * @return
     */
    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }
    //从transientStorePool中弹出一个预创建的buffer，并赋给writeBuffer
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        //创建一块文件大小的虚拟内存
        init(fileName, fileSize);
        //从buffer线程池中分配一块对应的堆内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;//文件名称
        this.fileSize = fileSize;//文件大小
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        //确认磁盘信息
        ensureDirOK(this.file.getParent());

        try {
            //创建一个文件管道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //mappedByteBuffer是一个使用NIO的内存buffer，并映射到物理文件commitlog
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }
    //获得文件的最新更新时间
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }
    //文件大小
    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /***
     *
     * @param msg 待追加到MappedFile的消息
     * @param cb 回调函数
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /***
     *
     * @param messageExt
     * @param cb
     * @return
     * 开始写入消息，并传递回调函数。此时消息只是写入到buffer缓冲区，分为单条写入和批量写入。每次写入消息后，最新写入的位置wrotePosition都会相应的更新
     *
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        //MappedFile中当前已写入的位置，也就是当前指针
        int currentPos = this.wrotePosition.get();
        //如果当前指针大于或等于文件大小，表示文件已写满
        if (currentPos < this.fileSize) {//当前文件还未写满
            //slice()方法会返回一个新的buffer，但是新的byteBuffer和源对象byteBuffer引用的是同一个。这里创建一个与MapperFile共享的内存区，
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);//设置这个共享区的当前指针
            AppendMessageResult result = null;
            if (messageExt instanceof MessageExtBrokerInner) {//单条消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {//批量消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            //更新最新写入的位置wrotePosition
            this.wrotePosition.addAndGet(result.getWroteBytes());
            //更新最新写入的时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘 直接调用mappedByteBuffer或者fileChannel将内存中的数据刷到磁盘里
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /***
     * 将commitLog冲刷到FileChannel。
     *  这里只有打开缓冲池的话才会调用commit方法，因为只有采用直接内存池，writeBuffer才不会为空。
     *
     * 注意，提交只是 把它从直接内存提交到文件内存映射中，也就是虚拟内存
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {//直接返回wrotePosition指针，无需commit，说明commit的主体实际上是writeBuffer
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {//判断是否满足本次最小的提交页数，如果不满足，则不执行本次提交操作
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();//当缓冲区提交到文件的内存映射里，则释放对该mappedFile的引用
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        //返回最新的已提交的偏移量
        return this.committedPosition.get();
    }

    /***
     * 首先创建writeBuffer共享缓冲区
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();//获得已写的偏移量
        int lastCommittedPosition = this.committedPosition.get();//获得已提交的偏移量
        //如果未提交的偏移量大于0，将还为未提交的偏移量写入到文件的fileChannel里
        if (writePos - this.committedPosition.get() > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();//首先创建writeBuffer共享缓冲区
                byteBuffer.position(lastCommittedPosition);//将创建的byteBuffer回溯到上一次commit的position
                byteBuffer.limit(writePos);//然后设置limiit为writePos(当前最大有效数据指针)
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);//然后把lastCommittedPosition到writePos的有效数据复制到文件通道fileChannel
                this.committedPosition.set(writePos);//更新committedPoisition为writePos
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /***
     * 判断满足刷盘的条件
     * 1、文件满了
     * 2、设置了flushLeastPages，则必须满足最少页
     * 3、如果没设置flushLeastPages，这个直接刷盘
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        //获得上一次刷盘的偏移量
        int flush = this.flushedPosition.get();
        //获得已写偏移量
        int write = getReadPosition();
        //文件满了，肯定刷盘啊
        if (this.isFull()) {
            return true;
        }
        //如果设置了flushLeastPages，待刷盘的偏移量满足最小刷盘的页闲置，则刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否可提交
     *       1、文件满了
     *      * 2、设置了commitLeastPages，则必须满足最少页
     *      * 3、如果没设置commitLeastPages，这个直接刷盘
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();
        //如果本文件满了，直接就可以提交
        if (this.isFull()) {
            return true;
        }
        //计算是否满足本次最小的提交页数
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    /**
     * 获得已落盘的偏移量
     * @return
     */
    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     *  //返回从pos到 pos + size的内存映射
     * @param pos 指定开始位置
     * @param size 指定大小
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {//引用+1
                //切片,复制一个byteBuffer(与原byteBuffer共享数据, 只是指针位置独立)
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                //设置结束位置,byteBufferNew的[0,size) 与 原有的mappedByteBuffer的[pos,pos+size)相同
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /***
     *
     *  //返回从pos到最大有效位置的所有数据
     * @param pos 选取buffer的开始位置
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();//获取当前有效数据的最大位置
        if (pos < readPosition && pos >= 0) {//返回的数据 必须是有效的
            if (this.hold()) {//引用+1
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;//剩下的都返回
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);//绝对offset,
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {//如果文件还是可用，肯定不能清除缓存
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }
        //再次判断，文件是否清理过，如果是的话，直接返回true
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }
        //开始清除虚拟内存
        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /***
     * 先释放资源
     * 再关闭通道
     * 最后删除文件
     * @param intervalForcibly
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {//判断是否清理完成，只有已经清理完成了，且不可用了，才能销毁
            try {
                this.fileChannel.close();//关闭通道
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();//删除文件
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 如果是采用缓冲池，则获得缓冲区的怼内存中已写的维护
     * 不然就获得虚拟内存映射区的提交位置
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /***
     * 这里 MappedFile 已经创建，对应的 Buffer 为 mappedByteBuffer。
     * mappedByteBuffer 已经通过 mmap 映射，此时操作系统中只是记录了该文件和该 Buffer 的映射关系，而没有映射到物理内存中。
     * 这里就通过对该 MappedFile 的每个 Page Cache 进行写入一个字节，通过读写操作把 mmap 映射全部加载到物理内存中。
     *
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 如果是同步写盘操作，则进行强行刷盘操作
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        // 把剩余的数据强制刷新到磁盘中
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 该方法主要是实现文件预热后，防止把预热过的文件被操作系统调到swap空间中。当程序在次读取交换出去的数据的时候会产生缺页异常。
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
