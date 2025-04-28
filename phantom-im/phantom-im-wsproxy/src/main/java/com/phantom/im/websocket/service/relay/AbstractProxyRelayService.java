/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phantom.im.websocket.service.relay;

import com.phantom.im.websocket.common.ProxyContext;
import com.phantom.im.websocket.common.ProxyException;
import com.phantom.im.websocket.common.ProxyExceptionCode;
import com.phantom.im.websocket.common.utils.ProxyUtils;
import com.phantom.im.websocket.service.transaction.TransactionData;
import com.phantom.im.websocket.service.transaction.TransactionService;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractProxyRelayService implements ProxyRelayService {

    protected final TransactionService transactionService;

    public AbstractProxyRelayService(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Override
    public RelayData<TransactionData, Void> processCheckTransactionState(ProxyContext context,
                                                                         RemotingCommand command, CheckTransactionStateRequestHeader header, MessageExt messageExt) {
        CompletableFuture<ProxyRelayResult<Void>> future = new CompletableFuture<>();
        String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        TransactionData transactionData = transactionService.addTransactionDataByBrokerAddr(
            context,
            command.getExtFields().get(ProxyUtils.BROKER_ADDR),
            messageExt.getTopic(),
            group,
            header.getTranStateTableOffset(),
            header.getCommitLogOffset(),
            header.getTransactionId(),
            messageExt);
        if (transactionData == null) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR,
                String.format("add transaction data failed. request:%s, message:%s", command, messageExt));
        }
        future.exceptionally(throwable -> {
            this.transactionService.onSendCheckTransactionStateFailed(context, group, transactionData);
            return null;
        });
        return new RelayData<>(transactionData, future);
    }
}
