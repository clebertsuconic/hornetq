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

import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.hornetq.amqp.dealer.AMQPClientConnection;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.dealer.SASL;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonInitializable;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.FutureRunnable;

/**
 * @author Clebert Suconic
 */

public class ProtonClientConnectionImpl extends ProtonAbstractConnectionImpl implements AMQPClientConnection
{
   public ProtonClientConnectionImpl(ProtonConnectionSPI connectionSPI)
   {
      super(connectionSPI);
   }

   // Maybe a client interface?
   public void clientOpen(SASL sasl) throws Exception
   {
      FutureRunnable future = new FutureRunnable(1);
      synchronized (trio.getLock())
      {
         this.afterInit(future);
         trio.createClientSasl(sasl);
         trio.getConnection().open();
         trio.dispatch();
      }

      waitWithTimeout(future);
   }

   public AMQPClientSession createClientSession() throws HornetQAMQPException
   {

      FutureRunnable futureRunnable =  new FutureRunnable(1);
      ProtonClientSessionImpl sessionImpl;
      synchronized (getTrio().getLock())
      {
         Session session = getTrio().getConnection().session();
         sessionImpl = (ProtonClientSessionImpl) getSession(session);
         sessionImpl.afterInit(futureRunnable);
         session.open();
         getTrio().dispatch();
      }

      waitWithTimeout(futureRunnable);

      return sessionImpl;
   }

   @Override
   protected  ProtonSessionImpl sessionOpened(Session realSession) throws HornetQAMQPException
   {
      ProtonSessionSPI sessionSPI = connectionSPI.createSessionSPI();
      ProtonSessionImpl protonSession = new ProtonClientSessionImpl(sessionSPI, this, realSession);
      realSession.setContext(protonSession);
      sessions.put(realSession, protonSession);

      return protonSession;

   }

   @Override
   protected void remoteLinkOpened(Link link) throws HornetQAMQPException
   {
      Object context = link.getContext();
      if (context != null && context instanceof ProtonInitializable)
      {
         ((ProtonInitializable)context).initialise();
      }
   }
}
