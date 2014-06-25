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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.protonimpl.AbstractProtonSender;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractReceiver;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.protonimpl.TransactionHandler;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */

public class ServerProtonSessionImpl extends ProtonSessionImpl
{

   public ServerProtonSessionImpl(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, Session session)
   {
      super(sessionSPI, connection, session);
   }

   protected Map<Object, AbstractProtonSender> serverSenders = new HashMap<Object, AbstractProtonSender>();


   /**
    * The consumer object from the broker or the key used to store the sender
    * @param message
    * @param consumer
    * @param deliveryCount
    * @return the number of bytes sent
    */
   public int serverDelivery(Object message, Object consumer, int deliveryCount)
   {
      ProtonServerSenderImpl protonSender = (ProtonServerSenderImpl)serverSenders.get(consumer);
      if (protonSender != null)
      {
         return protonSender.handleDelivery(message, deliveryCount);
      }
      return 0;
   }

   public void addTransactionHandler(Coordinator coordinator, Receiver receiver)
   {
      TransactionHandler transactionHandler = new TransactionHandler(sessionSPI);
      receiver.setContext(transactionHandler);
      receiver.open();
      receiver.flow(100);
   }

   public void addSender(Sender sender) throws HornetQAMQPException
   {
      ProtonServerSenderImpl protonSender = new ProtonServerSenderImpl(connection, sender, this, sessionSPI);

      try
      {
         protonSender.initialise();
         senders.put(sender, protonSender);
         serverSenders.put(protonSender.getBrokerConsumer(), protonSender);
         sender.setContext(protonSender);
         sender.open();
         protonSender.start();
      }
      catch (HornetQAMQPException e)
      {
         senders.remove(sender);
         sender.setSource(null);
         sender.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         sender.close();
      }
   }

   public void removeSender(Sender sender) throws HornetQAMQPException
   {
      ProtonServerSenderImpl senderRemoved = (ProtonServerSenderImpl)senders.remove(sender);
      if (senderRemoved != null)
      {
         serverSenders.remove(senderRemoved.getBrokerConsumer());
      }
   }


   public void addReceiver(Receiver receiver) throws HornetQAMQPException
   {
      try
      {
         ProtonAbstractReceiver protonReceiver = new ProtonServerReceiver(sessionSPI, connection, this, receiver);
         protonReceiver.initialise();
         receivers.put(receiver, protonReceiver);
         receiver.setContext(protonReceiver);
         receiver.open();
      }
      catch (HornetQAMQPException e)
      {
         receivers.remove(receiver);
         receiver.setTarget(null);
         receiver.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         receiver.close();
      }
   }

}
