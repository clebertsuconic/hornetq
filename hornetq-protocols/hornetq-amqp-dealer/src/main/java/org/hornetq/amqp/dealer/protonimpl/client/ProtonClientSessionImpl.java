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

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.hornetq.amqp.dealer.AMQPClientReceiver;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.FutureRunnable;

/**
 * @author Clebert Suconic
 */

public class ProtonClientSessionImpl extends ProtonSessionImpl implements AMQPClientSession
{
   public ProtonClientSessionImpl(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, Session session)
   {
      super(sessionSPI, connection, session);
   }

   public AMQPClientSender createSender(String address, boolean preSettled) throws HornetQAMQPException
   {
      FutureRunnable futureRunnable =  new FutureRunnable(1);

      ProtonClientSender amqpSender;
      synchronized (connection.getTrio().getLock())
      {
         Sender sender = session.sender(address);
         sender.setSenderSettleMode(SenderSettleMode.SETTLED);
         Target target = new Target();
         target.setAddress(address);
         sender.setTarget(target);
         amqpSender = new ProtonClientSender(connection, sender, this, sessionSPI);
         amqpSender.afterInit(futureRunnable);
         sender.setContext(amqpSender);
         sender.open();
         connection.getTrio().dispatch();
      }

      waitWithTimeout(futureRunnable);
      return amqpSender;
   }

   public AMQPClientReceiver createReceiver(String address) throws HornetQAMQPException
   {
      FutureRunnable futureRunnable =  new FutureRunnable(1);

      ProtonClientReceiver amqpReceiver;

      synchronized (connection.getTrio().getLock())
      {
         Receiver receiver = session.receiver(address);
         Source source = new Source();
         source.setAddress(address);
         receiver.setSource(source);
         amqpReceiver = new ProtonClientReceiver(sessionSPI, connection, this, receiver);
         receiver.setContext(amqpReceiver);
         amqpReceiver.afterInit(futureRunnable);
         receiver.open();
         connection.getTrio().dispatch();
      }

      waitWithTimeout(futureRunnable);

      return amqpReceiver;

   }
}
