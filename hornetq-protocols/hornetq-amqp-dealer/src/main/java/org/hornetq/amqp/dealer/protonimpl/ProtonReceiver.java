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

package org.hornetq.amqp.dealer.protonimpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.logger.HornetQAMQPProtocolMessageBundle;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         <p/>
 *         handles incoming messages via a Proton Receiver and forwards them to HornetQ
 */
public class ProtonReceiver implements ProtonDeliveryHandler
{
   private final ProtonAbstractConnectionImpl connection;

   private final ProtonSessionImpl protonSession;

   private final Receiver receiver;

   private final String address;

   private final ProtonSessionSPI sessionSPI;

   public ProtonReceiver(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, ProtonSessionImpl protonSession, Receiver receiver)
   {
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.address = receiver.getRemoteTarget().getAddress();
      this.sessionSPI = sessionSPI;
   }

   /*
   * called when Proton receives a message to be delivered via a Delivery.
   *
   * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
   *
   * */
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      Receiver receiver;
      try
      {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable())
         {
            return;
         }

         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024);
         try
         {
            synchronized (connection.getTrio().getLock())
            {
               int count;
               //todo an optimisation here would be to only use the buffer if we need more that one recv
               while ((count = receiver.recv(buffer.array(), buffer.arrayOffset() + buffer.writerIndex(), buffer.writableBytes())) > 0)
               {
                  // Increment the writer index by the number of bytes written into it while calling recv.
                  buffer.writerIndex(buffer.writerIndex() + count);
               }

               // we keep reading until we get end of messages, i.e. -1
               if (count == 0)
               {
                  // todo this is obviously incorrect, investigate return;
               }

               sessionSPI.serverSend(address, delivery.getMessageFormat(), buffer.nioBuffer());

               receiver.advance();

               receiver.flow(1);
               delivery.disposition(Accepted.getInstance());
               delivery.settle();

            }
         }
         finally
         {
            buffer.release();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();
         condition.setCondition(Symbol.valueOf("failed"));
         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         delivery.disposition(rejected);
      }
   }

   @Override
   public void checkState()
   {
      //no op
   }

   @Override
   public void close() throws HornetQAMQPException
   {
      protonSession.removeReceiver(receiver);
   }

   public void init() throws HornetQAMQPException
   {
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      if (target.getDynamic())
      {
         //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
         // will be deleted on closing of the session
         String queue = sessionSPI.tempQueueName();


         sessionSPI.createTemporaryQueue(queue);
         target.setAddress(queue.toString());
      }
      else
      {
         //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
         //be a queue bound to it so we nee to check this.
         String address = target.getAddress();
         if (address == null)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.targetAddressNotSet();
         }
         try
         {
            if (!sessionSPI.queueQuery(address))
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
            }
         }
         catch (Exception e)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorFindingTemporaryQueue(e.getMessage());
         }
      }
   }
}
