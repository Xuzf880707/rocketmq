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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    //延迟的broker
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    //是否开启故意延迟机制
    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /***
     *
     * @param tpInfo 主题信息
     * @param lastBrokerName 上一次轮询的broker
     * @return
     * 1、如果打开容错策略
     *      a、通过RocketMQ的预测机制来预测一个Broker是否可用，如果是重试的话，判断如果上次失败的Broker可用那么还是会选择该Broker的队列
     *      b、如果上次失败的broker就是不可用，则抛弃broker==lastBrokerName，则随机选择一个可用的broker进行发送
     *      d、如果通过上面没有找到合适的broker,则舍弃broker==lastBrokerName，选择一个可用的broker来发送。
     *          查询从规避的broker列表中选择一个可用的broker，如果没有找到，则返回null
     * 2、如果没有打开容错策略
     *      轮询队列进行发送，如果失败了，重试的时候过滤失败的Broker
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {//如果打开容错策略
            try {
                ////获取一个可用的并且brokerName=lastBrokerName的消息队列
                int index = tpInfo.getSendWhichQueue().getAndIncrement();//获得当前轮询值，并获得此次轮询到队列，是一个递增参数，每选择一次队列，值加1
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //判断轮询的队列所属的broker是否可用，
                    //判断broker是否已被加入faultItemTable？如果没有被加入，则表示该broker可用
                    // 如果broker已被加入faultItemTable，则判断是否已超过指定的规避时间，如果超过了规避时间，则返回true，表示该broker重新恢复可用。
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {//判断broker是否可用
                        //如果是重试的话，则判断选中的可用的brokerName和上一次的brokerName是否同一个，不是的话继续遍历下一个broker。
                        //如果不是重试的话，则直接返回选中的队列
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }
                //如果上面第一步中没有找到合适的队列，此时舍弃broker==lastBrokerName这个条件，选择一个相对较好的broker来发送
                //尝试从规避的broker中选择一个可用的Broker，如果没有找到，则返回null.不考虑可用性的消息队列
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();//获得一个规避的可用的broker
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);//从broker中查看写队列的个数
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {//从规避的broker列表中移除
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //在不开启容错的情况下，轮询队列进行发送，如果失败了，重试的时候过滤失败的Broker
            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }
    /***
     *
     * @param brokerName broker
     * @param currentLatency 请求到响应的实际消耗时间
     * @param isolation 发送成功：isolation=false，发送失败：isolation=true
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {//如果允许容错策略
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
