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

package org.hornetq.amqp.dealer.protonimpl.client;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.amqp.dealer.AMQPClientReceiver;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractReceiver;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */
public class ProtonClientReceiver extends ProtonAbstractReceiver implements AMQPClientReceiver
{
   public ProtonClientReceiver(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, ProtonSessionImpl protonSession, Receiver receiver)
   {
      super(sessionSPI, connection, protonSession, receiver);
   }

   /*
   * called when Proton receives a message to be delivered via a Delivery.
   *
   * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
   *
   * */
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      System.out.println("Receiving message " + delivery);


      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024);
      try
      {
         synchronized (connection.getTrio().getLock())
         {
            readDelivery(receiver, buffer);

            MessageImpl serverMessage = (MessageImpl)Message.Factory.create();
            serverMessage.decode(buffer.nioBuffer());


            System.out.println("message:" + serverMessage.getBody());

         }
      }
      finally
      {
         buffer.release();
      }
   }

   public void flow(int credits)
   {
      synchronized (connection.getTrio().getLock())
      {
         receiver.flow(credits);
         connection.getTrio().dispatch();
      }
   }


   @Override
   public ProtonJMessage receiveMessage(int time, TimeUnit unit)
   {
      return null;
   }
}
