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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 * 平均分配，默认
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /***
     *  为消费者currentCID分配队列
     * @param consumerGroup current consumer group  消费者组
     * @param currentCID current consumer id 当前的消费者id
     * @param mqAll message queue set in current topic 所有的mq
     * @param cidAll consumer set in current consumer group 当前消费者组中所有的消费者，保存的是消费者id
     * @return
     *      1、获取当前消费者id，并做有效性判断
     *      2、主题中不能没有消费者队列
     *      3、判断消费者在消费者组中的索引下标
     *      4、对topic所有的mq和消费者组中所有的消费者取模
     *      5、如果topic的mq数比消费者还少，则每个消费者最多分配一个对列；
     *          如果topic的mq数大于消费者，则可能消费者id大的消费者可能会多分配一个
     *
     *      6、为消费者分配消息队列的时候，如下：加入有消费者 a、b、c，有队列 1、2、3、4、5、6、7、8
     *          则
     *              a：1、2
     *              b：3、4、5
     *              c：6、7、8
     */

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        int index = cidAll.indexOf(currentCID);//将消费者组有序排列成一个列表，每个消费者在消费者组中都有一个下标，这个index就代表下标
        int mod = mqAll.size() % cidAll.size();//判断messagequeue对消费者进行均分后剩余多少个messqgeQueue
        /**
         * 1、如果 messageQueue 比消费者个数还少，则消费者最多只会分配到一个messageQueue。
         * 2、如果 messageQueue 比消费者个数多
         *      如果不整除，也就是mod>0:
         *          如果当前的消费者所属的索引比余数还小，则该消费者多分配一个队列：mqAll.size() / cidAll.size() +1。
         *          如果当前的消费者所属的索引比余数大，则该消费者只会分配mqAll.size() / cidAll.size()。
         *      如果整除：则该消费者只会分配mqAll.size() / cidAll.size()。
         * 3、如果不整除且索引下标小于余数，则
         */
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
