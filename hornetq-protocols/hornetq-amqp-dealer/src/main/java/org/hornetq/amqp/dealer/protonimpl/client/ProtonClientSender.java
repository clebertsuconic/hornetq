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

import org.apache.qpid.amqp_1_0.type.messaging.Accepted;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.protonimpl.AbstractProtonSender;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.FutureRunnable;

/**
 * @author Clebert Suconic
 */

public class ProtonClientSender extends AbstractProtonSender implements AMQPClientSender
{

   FutureRunnable catchUpRunnable = new FutureRunnable();

   public ProtonClientSender(ProtonAbstractConnectionImpl connection, Sender sender, ProtonSessionImpl protonSession, ProtonSessionSPI server)
   {
      super(connection, sender, protonSession, server);
   }

   @Override
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      if (delivery.getRemoteState() instanceof Accepted)
      {
         if (delivery.getContext() instanceof FutureRunnable)
         {
            ((FutureRunnable)delivery.getContext()).countDown();
         }
      }
   }

   public void send(ProtonJMessage message)
   {
      if (sender.getSenderSettleMode() != SenderSettleMode.SETTLED)
      {
         catchUpRunnable.countUp();
      }
      performSend(message, catchUpRunnable);
   }


   public boolean sync(long timeout, TimeUnit unit)
   {
      try
      {
         return catchUpRunnable.await(timeout, unit);
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
         return false;
      }

   }

}
