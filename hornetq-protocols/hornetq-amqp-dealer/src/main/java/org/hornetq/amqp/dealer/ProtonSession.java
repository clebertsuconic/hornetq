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

package org.hornetq.amqp.dealer;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPInternalErrorException;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/10/13
 */
public class ProtonSession
{
   private final ProtonRemotingConnection connection;

   private final ProtonSessionSPI sessionSPI;

   private long currentTag = 0;

   private Map<Object, ProtonInbound> producers = new HashMap<Object, ProtonInbound>();

   private Map<Object, ProtonOutbound> consumers = new HashMap<Object, ProtonOutbound>();

   private boolean closed = false;

   private boolean initialized = false;

   public ProtonSession(ProtonSessionSPI sessionSPI, ProtonRemotingConnection connection)
   {
      this.connection = connection;
      this.sessionSPI = sessionSPI;
   }

   /*
   * we need to initialise the actual server session when we receive the first linkas this tells us whether or not the
   * session is transactional
   * */
   public void initialise(boolean transacted) throws HornetQAMQPInternalErrorException
   {
      if (!initialized)
      {
         try
         {
            sessionSPI.init(this, connection.getLogin(), connection.getPasscode(), transacted);
            initialized = true;
         }
         catch (Exception e)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCreatingHornetQSession(e.getMessage());
         }
      }
   }

   public int deliverMessage(Object message, Object consumer, int deliveryCount)
   {
      ProtonOutbound protonConsumer = consumers.get(consumer);
      if (protonConsumer != null)
      {
         return protonConsumer.handleDelivery(message, deliveryCount);
      }
      return 0;
   }

   public void disconnect(Object consumer, String queueName)
   {
      ProtonOutbound protonConsumer = consumers.remove(consumer);
      if (protonConsumer != null)
      {
         try
         {
            protonConsumer.close();
         }
         catch (HornetQAMQPException e)
         {
            protonConsumer.getSender().setTarget(null);
            protonConsumer.getSender().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
      }
   }

   public void addProducer(Receiver receiver) throws HornetQAMQPException
   {
      try
      {
         ProtonInbound producer = new ProtonInbound(sessionSPI, connection, this, receiver);
         producer.init();
         producers.put(receiver, producer);
         receiver.setContext(producer);
         receiver.open();
      }
      catch (HornetQAMQPException e)
      {
         producers.remove(receiver);
         receiver.setTarget(null);
         receiver.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         receiver.close();
      }
   }


   public void addTransactionHandler(Coordinator coordinator, Receiver receiver)
   {
      TransactionHandler transactionHandler = new TransactionHandler(sessionSPI);
      receiver.setContext(transactionHandler);
      receiver.open();
      receiver.flow(100);
   }

   public void addConsumer(Sender sender) throws HornetQAMQPException
   {
      ProtonOutbound protonConsumer = new ProtonOutbound(connection, sender, this, sessionSPI);

      try
      {
         protonConsumer.init();
         consumers.put(protonConsumer.getBrokerConsumer(), protonConsumer);
         sender.setContext(protonConsumer);
         sender.open();
         protonConsumer.start();
      }
      catch (HornetQAMQPException e)
      {
         consumers.remove(protonConsumer.getBrokerConsumer());
         sender.setSource(null);
         sender.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         sender.close();
      }
   }

   public byte[] getTag()
   {
      return Long.toHexString(currentTag++).getBytes();
   }

   public void replaceTag(byte[] tag)
   {
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      for (ProtonInbound protonProducer : producers.values())
      {
         try
         {
            protonProducer.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            // TODO Logging
         }
      }
      producers.clear();
      for (ProtonOutbound protonConsumer : consumers.values())
      {
         try
         {
            protonConsumer.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            // TODO Logging
         }
      }
      consumers.clear();
      try
      {
         sessionSPI.rollbackCurrentTX();
         sessionSPI.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         // TODO logging
      }
      closed = true;
   }

   public void removeConsumer(Object brokerConsumer) throws HornetQAMQPException
   {
      consumers.remove(brokerConsumer);
   }

   public void removeProducer(Receiver receiver)
   {
      producers.remove(receiver);
   }
}
