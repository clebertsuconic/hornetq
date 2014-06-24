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

package org.hornetq.amqp.dealer.protonimpl.server;

import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.impl.LinkImpl;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.logger.HornetQAMQPProtocolMessageBundle;
import org.hornetq.amqp.dealer.protonimpl.AbstractProtonSender;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.NettyWritable;
import org.hornetq.amqp.dealer.util.ProtonServerMessage;
import org.apache.qpid.proton.amqp.messaging.Source;

/**
 * @author Clebert Suconic
 */

public class ProtonServerSenderImpl extends AbstractProtonSender
{

   private static final Symbol SELECTOR = Symbol.getSymbol("jms-selector");
   private static final Symbol COPY = Symbol.valueOf("copy");

   private Object brokerConsumer;

   public ProtonServerSenderImpl(ProtonAbstractConnectionImpl connection, Sender sender, ProtonSessionImpl protonSession, ProtonSessionSPI server)
   {
      super(connection, sender, protonSession, server);
   }

   public Object getBrokerConsumer()
   {
      return brokerConsumer;
   }

   /*
* start the session
* */
   public void start() throws HornetQAMQPException
   {
      super.start();
      // protonSession.getServerSession().start();

      //todo add flow control
      try
      {
         // to do whatever you need to make the broker start sending messages to the consumer
         sessionSPI.startConsumer(brokerConsumer);
         //protonSession.getServerSession().receiveConsumerCredits(consumerID, -1);
      }
      catch (Exception e)
      {
         throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorStartingConsumer(e.getMessage());
      }
   }

   /**
    * create the actual underlying HornetQ Server Consumer
    * */
   @Override
   public void initialise() throws HornetQAMQPException
   {
      super.initialise();

      Source source = (Source) sender.getRemoteSource();

      String queue;

      String selector = null;
      Map filter = source.getFilter();
      if (filter != null)
      {
         DescribedType value = (DescribedType) filter.get(SELECTOR);
         if (value != null)
         {
            selector = value.getDescribed().toString();
         }
      }

      if (source.getDynamic())
      {
         //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
         // will be deleted on closing of the session
         queue = java.util.UUID.randomUUID().toString();
         try
         {
            sessionSPI.createTemporaryQueue(queue);
            //protonSession.getServerSession().createQueue(queue, queue, null, true, false);
         }
         catch (Exception e)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
         }
         source.setAddress(queue);
      }
      else
      {
         //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
         //be a queue bound to it so we nee to check this.
         queue = source.getAddress();
         if (queue == null)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.sourceAddressNotSet();
         }

         if (!sessionSPI.queueQuery(queue))
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
         }
      }

      boolean browseOnly = source.getDistributionMode() != null && source.getDistributionMode().equals(COPY);
      try
      {
         brokerConsumer = sessionSPI.createConsumer(queue, selector, browseOnly);
      }
      catch (Exception e)
      {
         throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCreatingHornetQConsumer(e.getMessage());
      }
   }

   /*
   * close the session
   * */
   public void close() throws HornetQAMQPException
   {
      super.close();
      sessionSPI.closeConsumer(brokerConsumer);
   }


   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      Object message = delivery.getContext();

      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;


      DeliveryState remoteState = delivery.getRemoteState();

      if (remoteState != null)
      {
         if (remoteState instanceof Accepted)
         {
            //we have to individual ack as we can't guarantee we will get the delivery updates (including acks) in order
            // from dealer, a perf hit but a must
            try
            {
               sessionSPI.ack(brokerConsumer, message);
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
            }
         }
         else if (remoteState instanceof Released)
         {
            try
            {
               sessionSPI.cancel(brokerConsumer, message, false);
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         else if (remoteState instanceof Rejected || remoteState instanceof Modified)
         {
            try
            {
               sessionSPI.cancel(brokerConsumer, message, true);
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         //todo add tag caching
         if (!preSettle)
         {
            protonSession.replaceTag(delivery.getTag());
         }

         synchronized (connection.getTrio().getLock())
         {
            delivery.settle();
            sender.offer(1);
         }

      }
      else
      {
         //todo not sure if we need to do anything here
      }
   }

   @Override
   public synchronized void checkState()
   {
      super.checkState();
      sessionSPI.resumeDelivery(brokerConsumer);
   }


   /**
    * handle an out going message from HornetQ, send via the Proton Sender
    * */
   public int handleDelivery(Object message, int deliveryCount)
   {
      if (closed)
      {
         System.err.println("Message can't be delivered as it's closed");
         return 0;
      }

      //presettle means we can ack the message on the dealer side before we send it, i.e. for browsers
      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;
      //we only need a tag if we are going to ack later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();
      //encode the message
      ProtonServerMessage serverMessage = null;
      try
      {
         // This can be done a lot better here
         serverMessage = sessionSPI.encodeMessage(message, deliveryCount);
      }
      catch (Throwable e)
      {
         e.printStackTrace();
      }

      ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024);
      try
      {
         serverMessage.encode(new NettyWritable(nettyBuffer));

         int size = nettyBuffer.writerIndex();

         synchronized (connection.getTrio().getLock())
         {
            final Delivery delivery;
            delivery = sender.delivery(tag, 0, tag.length);
            delivery.setContext(message);

            // this will avoid a copy.. patch provided by Norman using buffer.array()
            sender.send(nettyBuffer.array(), nettyBuffer.arrayOffset() + nettyBuffer.readerIndex(), nettyBuffer.readableBytes());

            ((LinkImpl) sender).addCredit(1);

            if (preSettle)
            {
               delivery.settle();
            }
            else
            {
               sender.advance();
            }

            connection.getTrio().dispatch();
         }


         return size;
      }
      finally
      {
         nettyBuffer.release();
      }
   }


}
