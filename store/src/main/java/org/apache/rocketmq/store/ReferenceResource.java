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

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     *关闭 mappedFile
     * @param intervalForcibly 表示拒绝被销毁的最大时间
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {//如果当前文件可用，表示是初次关闭的，则需要先将当前文件设置成不可用
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();//设置初次关闭的时间
            this.release();//释放资源，release只有在引用数小于1的时候才会释放资源
        } else if (this.getRefCount() > 0) {//如果引用数仍然大于0
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {//如果已经超过了其最大的拒绝销毁时间，则引用数直接设置为-1000，这样就可以释放资源了
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放对该文件的引用的，同时会更新总的虚拟内存和虚拟内存文件的统计
     */
    public void release() {
        //获得当前对当前文件的引用
        long value = this.refCount.decrementAndGet();
        if (value > 0)//如果引用数大于0的话，则不能被释放资源，直接返回
            return;

        synchronized (this) {
            //更新虚拟内存的额统计和虚拟内存文件数的统计
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
