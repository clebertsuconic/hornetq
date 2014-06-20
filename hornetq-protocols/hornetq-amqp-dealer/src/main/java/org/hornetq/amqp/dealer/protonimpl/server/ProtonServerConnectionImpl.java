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

import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */

public class ProtonServerConnectionImpl extends ProtonAbstractConnectionImpl
{
   public ProtonServerConnectionImpl(ProtonConnectionSPI connectionSP)
   {
      super(connectionSP);
   }

   public void createServerSASL()
   {
      trio.createServerSasl(connectionSPI.getSASLMechanisms());
   }



   protected ProtonSessionImpl sessionOpened(Session realSession) throws HornetQAMQPException
   {
      ProtonSessionSPI sessionSPI = connectionSPI.createSessionSPI();
      ProtonSessionImpl protonSession = new ServerProtonSessionImpl(sessionSPI, this, realSession);
      realSession.setContext(protonSession);
      sessions.put(realSession, protonSession);

      return protonSession;
   }

   protected void remoteLinkOpened(Link link) throws HornetQAMQPException
   {

      ServerProtonSessionImpl protonSession = (ServerProtonSessionImpl)getSession(link.getSession());

      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver)
      {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator)
         {
            protonSession.setTransacted(true);
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         }
         else
         {
            protonSession.setTransacted(false);
            protonSession.addReceiver(receiver);
            //todo do this using the server session flow control
            receiver.flow(100);
         }
      }
      else
      {
         synchronized (getTrio().getLock())
         {
            protonSession.setTransacted(false);
            Sender sender = (Sender) link;
            protonSession.addSender(sender);
            sender.offer(1);
         }
      }
   }

}
