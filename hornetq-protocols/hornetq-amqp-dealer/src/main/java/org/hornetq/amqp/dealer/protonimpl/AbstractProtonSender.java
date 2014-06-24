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

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * A this is a wrapper around a HornetQ ServerConsumer for handling outgoing messages and incoming acks via a Proton Sender
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class AbstractProtonSender extends ProtonInitializable implements ProtonDeliveryHandler
{
   protected final ProtonSessionImpl protonSession;
   protected final Sender sender;
   protected final ProtonAbstractConnectionImpl connection;
   protected boolean closed = false;
   protected final ProtonSessionSPI sessionSPI;

   public AbstractProtonSender(ProtonAbstractConnectionImpl connection, Sender sender, ProtonSessionImpl protonSession, ProtonSessionSPI server)
   {
      this.connection = connection;
      this.sender = sender;
      this.protonSession = protonSession;
      this.sessionSPI = server;
   }


   /*
   * start the session
   * */
   public void start() throws HornetQAMQPException
   {
      sessionSPI.start();
      // protonSession.getServerSession().start();
   }

   /*
   * close the session
   * */
   public void close() throws HornetQAMQPException
   {
      closed = true;
      protonSession.removeSender(sender);
      synchronized (connection.getTrio().getLock())
      {
         sender.close();
         connection.getTrio().dispatch();
      }
   }

   @Override
   /*
   * handle an incoming Ack from Proton, basically pass to HornetQ to handle
   * */
   public abstract void onMessage(Delivery delivery) throws HornetQAMQPException;

   /*
   * check the state of the consumer, i.e. are there any more messages. only really needed for browsers?
   * */
   public void checkState()
   {
   }

   public Sender getSender()
   {
      return sender;
   }
}
