/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.amqp.dealer.spi;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.amqp.Binary;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.util.ProtonServerMessage;

/**
 * These are methods where the Proton Plug component will call your server
 * @author Clebert Suconic
 */

public interface ProtonSessionSPI
{

   void init(ProtonSessionImpl session, String user, String passcode, boolean transacted);

   void start();

   Object createConsumer(String queue, String filer, boolean browserOnly);

   void startConsumer(Object brokerConsumer);

   void createTemporaryQueue(String queueName);

   boolean queueQuery(String queueName);

   void closeConsumer(Object brokerConsumer);

   // This one can be a lot improved
   ProtonServerMessage encodeMessage(Object message, int deliveryCount);

   ByteBuf pooledBuffer(int size);

   Binary getCurrentTXID();

   String tempQueueName();

   void commitCurrentTX();

   void rollbackCurrentTX();

   void close();


   void ack(Object brokerConsumer, Object message);

   /**
    * @param brokerConsumer
    * @param message
    * @param updateCounts this identified if the cancel was because of a failure or just cleaning up the
    *                     client's cache.
    *                     in some implementations you could call this failed
    */
   void cancel(Object brokerConsumer, Object message, boolean updateCounts);


   void resumeDelivery(Object consumer);


   void serverSend(String address, int messageFormat, ByteBuffer messageEncoded);

}
