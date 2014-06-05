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

package org.hornetq.amqp.test.dumbserver;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.hornetq.amqp.dealer.ProtonSession;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */

public class MinimalSessionSPI implements ProtonSessionSPI
{

   String user;
   String password;
   boolean transacted;
   ProtonSession session;

   @Override
   public void init(ProtonSession session, String user, String passcode, boolean transacted)
   {
      this.session = session;
      this.user = user;
      this.password = passcode;
      this.transacted = transacted;
   }

   @Override
   public void start()
   {
   }

   @Override
   public Object createConsumer(String queue, String filer, boolean browserOnly)
   {
      Consumer consumer = new Consumer(DumbServer.getQueue(queue));
      return consumer;
   }

   @Override
   public void startConsumer(Object brokerConsumer)
   {
      ((Consumer)brokerConsumer).start();
   }

   @Override
   public void createTemporaryQueue(String queueName)
   {

   }

   @Override
   public boolean queueQuery(String queueName)
   {
      return true;
   }

   @Override
   public void closeConsumer(Object brokerConsumer)
   {
      ((Consumer)brokerConsumer).close();
   }

   @Override
   public EncodedMessage encodeMessage(Object message, int deliveryCount)
   {
      // We are storing internally as EncodedMessage on this minimal server
      return (EncodedMessage)message;
   }

   @Override
   public ByteBuf createBuffer(int size)
   {
      return UnpooledByteBufAllocator.DEFAULT.buffer(size);
   }

   @Override
   public ByteBuf pooledBuffer(int size)
   {
      return PooledByteBufAllocator.DEFAULT.buffer(size);
   }

   @Override
   public Binary getCurrentTXID()
   {
      return new Binary(new byte[]{1});
   }

   @Override
   public void commitCurrentTX()
   {
   }

   @Override
   public void rollbackCurrentTX()
   {
   }

   @Override
   public void close()
   {

   }

   @Override
   public void ack(Object brokerConsumer, Object message)
   {

   }

   @Override
   public void cancel(Object brokerConsumer, Object message, boolean updateCounts)
   {

   }

   @Override
   public void resumeDelivery(Object consumer)
   {
      ((Consumer)consumer).start();
   }

   @Override
   public void serverSend(EncodedMessage encodedMessage, String address)
   {
      DumbServer.getQueue(address).add(encodedMessage);
   }


   class Consumer
   {
      final BlockingDeque<Object> queue;

      Consumer(BlockingDeque<Object> queue)
      {
         this.queue = queue;
      }

      boolean running = false;
      volatile Thread thread;

      public void close()
      {
         running = false;
         if (thread != null)
         {
            try
            {
               thread.join();
            }
            catch (Throwable ignored)
            {
            }
         }

         thread = null;
      }

      public void start()
      {
         running = true;
         if (thread != null)
         {
            thread = new Thread()
            {
               public void run()
               {
                  try
                  {
                     while (running)
                     {
                        Object msg = queue.poll(1, TimeUnit.SECONDS);
                        if (msg != null)
                        {
                           session.deliverMessage(msg, Consumer.this, 1);
                        }
                     }
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            };
            thread.start();
         }
      }

   }
}
