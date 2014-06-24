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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.hornetq.amqp.dealer.AMQPConnection;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.util.ProtonTrio;

/**
 * Clebert Suconic
 */
public abstract class ProtonAbstractConnectionImpl extends ProtonInitializable implements AMQPConnection
{
   protected final ProtonInterceptTrio trio;
   protected final ProtonConnectionSPI connectionSPI;
   protected final long creationTime;

   protected final Map<Object, ProtonSessionImpl> sessions = new ConcurrentHashMap<>();
   protected volatile boolean dataReceived;

   public ProtonAbstractConnectionImpl(ProtonConnectionSPI connectionSPI)
   {
      this.connectionSPI = connectionSPI;
      this.creationTime = System.currentTimeMillis();
      trio = createTrio(connectionSPI);
   }

   protected ProtonInterceptTrio createTrio(ProtonConnectionSPI connectionSPI)
   {
      return new ProtonInterceptTrio(connectionSPI.newSingleThreadExecutor());
   }

   @Override
   public int inputBuffer(ByteBuf buffer)
   {
      setDataReceived();
      return trio.pump(buffer);
   }

   public ProtonTrio getTrio()
   {
      return trio;
   }

   public void destroy()
   {
      connectionSPI.close();
   }

   public ProtonConnectionSPI getTransportConnection()
   {
      return connectionSPI;
   }

   @Override
   public String getLogin()
   {
      return trio.getUsername();
   }

   @Override
   public String getPasscode()
   {
      return trio.getPassword();
   }

   protected ProtonSessionImpl getSession(Session realSession) throws HornetQAMQPException
   {
      ProtonSessionImpl protonSession = sessions.get(realSession);
      if (protonSession == null)
      {
         // how this is possible? Log a warn here
         return sessionOpened(realSession);
      }
      return protonSession;
   }

   protected abstract void remoteLinkOpened(Link link) throws HornetQAMQPException;


   protected abstract ProtonSessionImpl sessionOpened(Session realSession) throws HornetQAMQPException;

   @Override
   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public long getCreationTime()
   {
      return creationTime;
   }


   protected synchronized void setDataReceived()
   {
      dataReceived = true;
   }

   protected class ProtonInterceptTrio extends ProtonTrio
   {

      public ProtonInterceptTrio(Executor executor)
      {
         super(executor);
      }

      @Override
      protected void connectionOpened(Connection connection) throws Exception
      {
         initialise();
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
            ProtonAbstractConnectionImpl.this.getSession(session).initialise();
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
            remoteLinkOpened(link);
         }
         catch (Throwable e)
         {
            e.printStackTrace();
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
