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

package org.hornetq.amqp.dealer.impl;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.ProtonTrio;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * Clebert Suconic
 */
public class ProtonConnectionImpl
{
   private final ProtonServerTrio trio;

   private final Map<Object, ProtonSessionImpl> sessions = new ConcurrentHashMap<>();

   private final long creationTime;

   private boolean dataReceived;

   private final ProtonConnectionSPI connectionSPI;

   public ProtonConnectionImpl(ProtonConnectionSPI connectionSPI)
   {
      this.connectionSPI = connectionSPI;

      this.creationTime = System.currentTimeMillis();

      trio = new ProtonServerTrio(connectionSPI.newSingleThreadExecutor());

      trio.createServerSasl(connectionSPI.getSASLMechanisms());
   }

   public int inputBuffer(ByteBuf buffer)
   {
      return trio.pump(buffer);
   }

   public ProtonTrio getTrio()
   {
      return trio;
   }

   public long getCreationTime()
   {
      return creationTime;
   }

   public void destroy()
   {
      connectionSPI.close();
   }

   public ProtonConnectionSPI getTransportConnection()
   {
      return connectionSPI;
   }

   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   public String getLogin()
   {
      return trio.getUsername();
   }

   public String getPasscode()
   {
      return trio.getPassword();
   }

   protected synchronized void setDataReceived()
   {
      dataReceived = true;
   }

   private ProtonSessionImpl getSession(Session realSession) throws HornetQAMQPException
   {
      ProtonSessionImpl protonSession = sessions.get(realSession);
      if (protonSession == null)
      {
         // how this is possible? Log a warn here
         System.err.println("Couldn't find session, creating one");
         return createSession(realSession);
      }
      return protonSession;
   }


   private ProtonSessionImpl createSession(Session realSession) throws HornetQAMQPException
   {
      ProtonSessionSPI sessionSPI = connectionSPI.createSessionSPI();
      ProtonSessionImpl protonSession = new ProtonSessionImpl(sessionSPI, this);
      realSession.setContext(protonSession);
      sessions.put(realSession, protonSession);

      return protonSession;

   }

   class ProtonServerTrio extends ProtonTrio
   {

      public ProtonServerTrio(Executor executor)
      {
         super(executor);
      }

      @Override
      protected void connectionOpened(org.apache.qpid.proton.engine.Connection connection)
      {

      }

      @Override
      protected void connectionClosed(org.apache.qpid.proton.engine.Connection connection)
      {
         for (ProtonSessionImpl protonSession : sessions.values())
         {
            protonSession.close();
         }
         sessions.clear();
         // We must force write the channel before we actually destroy the connection
         onTransport(transport);
         destroy();

      }

      @Override
      protected void sessionOpened(Session session)
      {

         try
         {
            createSession(session);
         }
         catch (Throwable e)
         {
            session.close();
            transport.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, e.getMessage()));
         }

      }

      @Override
      protected void sessionClosed(Session session)
      {
         ProtonSessionImpl protonSession = (ProtonSessionImpl) session.getContext();
         protonSession.close();
         sessions.remove(session);
         session.close();
      }

      @Override
      protected void linkOpened(Link link)
      {

         try
         {

            ProtonSessionImpl protonSession = getSession(link.getSession());

            link.setSource(link.getRemoteSource());
            link.setTarget(link.getRemoteTarget());
            if (link instanceof Receiver)
            {
               Receiver receiver = (Receiver) link;
               if (link.getRemoteTarget() instanceof Coordinator)
               {
                  protonSession.initialise(true);
                  Coordinator coordinator = (Coordinator) link.getRemoteTarget();
                  protonSession.addTransactionHandler(coordinator, receiver);
               }
               else
               {
                  protonSession.initialise(false);
                  protonSession.addReceiver(receiver);
                  //todo do this using the server session flow control
                  receiver.flow(100);
               }
            }
            else
            {
               synchronized (getTrio().getLock())
               {
                  protonSession.initialise(false);
                  Sender sender = (Sender) link;
                  protonSession.addSender(sender);
                  sender.offer(1);
               }
            }
         }
         catch (Throwable e)
         {
            link.close();
            transport.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, e.getMessage()));
         }
      }

      @Override
      protected void linkClosed(Link link)
      {
         try
         {
            ((ProtonDeliveryHandler) link.getContext()).close();
         }
         catch (Throwable e)
         {
            link.close();
            transport.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, e.getMessage()));
         }

      }

      @Override
      protected void onDelivery(Delivery delivery)
      {
         ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
         try
         {
            if (handler != null)
            {
               handler.onMessage(delivery);
            }
            else
            {
               // TODO: logs

               System.err.println("Handler is null, can't delivery " + delivery);
            }
         }
         catch (HornetQAMQPException e)
         {
            delivery.getLink().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
      }


      @Override
      protected void linkActive(Link link)
      {
         try
         {
            link.setSource(link.getRemoteSource());
            link.setTarget(link.getRemoteTarget());
            ProtonDeliveryHandler handler = (ProtonDeliveryHandler) link.getContext();
            handler.checkState();
         }
         catch (Throwable e)
         {
            link.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, e.getMessage()));
         }
      }


      @Override
      protected void onTransport(Transport transport)
      {
         ByteBuf bytes = getPooledNettyBytes(transport);
         if (bytes != null)
         {
            // null means nothing to be written
            connectionSPI.output(bytes);
         }
      }

      /** return the current byte output */
      private ByteBuf getPooledNettyBytes(Transport transport)
      {
         int size = transport.pending();

         if (size <= 0)
         {
            return null;
         }

         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(size);

         ByteBuffer bufferInput = transport.head();

         buffer.writeBytes(bufferInput);

         transport.pop(size);

         return buffer;
      }

   }

}
