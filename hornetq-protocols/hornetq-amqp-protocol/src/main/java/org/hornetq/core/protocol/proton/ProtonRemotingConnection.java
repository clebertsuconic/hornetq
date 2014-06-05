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

package org.hornetq.core.protocol.proton;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.protocol.proton.client.MessageCreator;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.protocol.proton.util.ProtonTrio;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.core.protocol.proton.utils.ProtonUtils;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonRemotingConnection implements RemotingConnection, MessageCreator<ServerMessage>
{
   private final ProtonServerTrio trio;

   private final Map<Object, ProtonSession> sessions = new ConcurrentHashMap<>();

   private final ProtonUtils<ServerMessage, ProtonRemotingConnection> utils = new ProtonUtils<>();


   private boolean destroyed = false;

   private String clientId;

   private final Acceptor acceptorUsed;

   private final long creationTime;

   private final Connection remotingConnection;

   private final ProtonProtocolManager protonProtocolManager;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private boolean dataReceived;

   public ProtonRemotingConnection(Acceptor acceptorUsed, Connection connection, ProtonProtocolManager protonProtocolManager, Executor executor)
   {
      this.protonProtocolManager = protonProtocolManager;

      this.remotingConnection = connection;

      this.creationTime = System.currentTimeMillis();

      this.acceptorUsed = acceptorUsed;

      connection.setProtocolConnection(this);

      trio = new ProtonServerTrio(executor);

      trio.createServerSasl("PLAIN");
   }

   public ProtonUtils<ServerMessage, ProtonRemotingConnection> getUtils()
   {
      return utils;
   }

   public ProtonTrio getTrio()
   {
      return trio;
   }

   @Override
   public Object getID()
   {
      return remotingConnection.getID();
   }

   @Override
   public long getCreationTime()
   {
      return creationTime;
   }

   @Override
   public String getRemoteAddress()
   {
      return remotingConnection.getRemoteAddress();
   }

   @Override
   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      return closeListeners.remove(listener);
   }

   @Override
   public List<CloseListener> removeCloseListeners()
   {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   @Override
   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = new ArrayList<FailureListener>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   @Override
   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   @Override
   public void setFailureListeners(final List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   public List<FailureListener> getFailureListeners()
   {
      // we do not return the listeners otherwise the remoting service
      // would NOT destroy the connection.
      return Collections.emptyList();
   }

   @Override
   public HornetQBuffer createBuffer(int size)
   {
      return remotingConnection.createBuffer(size);
   }

   @Override
   public void fail(HornetQException me)
   {
      HornetQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      destroyed = true;

      remotingConnection.close();
   }

   @Override
   public void destroy()
   {
      destroyed = true;

      remotingConnection.close();

      callClosingListeners();
   }

   @Override
   public Connection getTransportConnection()
   {
      return remotingConnection;
   }

   @Override
   public boolean isClient()
   {
      return false;
   }

   @Override
   public boolean isDestroyed()
   {
      return destroyed;
   }

   @Override
   public void disconnect(final boolean criticalError)
   {
      disconnect(null, criticalError);
   }

   @Override
   public void disconnect(final TransportConfiguration tc, final boolean criticalError)
   {
      destroy();
   }

   @Override
   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public void flush()
   {
      //no op
   }

   @Override
   public void bufferReceived(Object connectionID, HornetQBuffer buffer)
   {
      protonProtocolManager.handleBuffer(this, buffer);
   }

   public String getLogin()
   {
      return trio.getUsername();
   }

   public String getPasscode()
   {
      return trio.getPassword();
   }

   public ServerMessageImpl createMessage()
   {
      return protonProtocolManager.createServerMessage();
   }

   protected synchronized void setDataReceived()
   {
      dataReceived = true;
   }

   private ProtonSession getSession(Session realSession) throws HornetQAMQPException
   {
      ProtonSession protonSession = sessions.get(realSession);
      if (protonSession == null)
      {
         protonSession = protonProtocolManager.createSession(this);
         sessions.put(realSession, protonSession);
      }
      return protonSession;

   }

   private void callFailureListeners(final HornetQException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me, false);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void callClosingListeners()
   {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(closeListeners);

      for (final CloseListener listener : listenersClone)
      {
         try
         {
            listener.connectionClosed();
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
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
         for (ProtonSession protonSession : sessions.values())
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
            ProtonSession protonSession = getSession(session);
            session.setContext(protonSession);
         }
         catch (HornetQException e)
         {
            session.close();
            transport.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, e.getMessage()));
         }

      }

      @Override
      protected void sessionClosed(Session session)
      {
         ProtonSession protonSession = (ProtonSession) session.getContext();
         protonSession.close();
         sessions.remove(session);
         session.close();
      }

      @Override
      protected void linkOpened(Link link)
      {
         try
         {
            protonProtocolManager.handleNewLink(link, getSession(link.getSession()), ProtonRemotingConnection.this);
         }
         catch (HornetQException e)
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
         catch (HornetQException e)
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
            protonProtocolManager.handleActiveLink(link);
         }
         catch (HornetQAMQPException e)
         {
            link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
      }


      @Override
      protected void onTransport(Transport transport)
      {
         ByteBuf bytes = getPooledNettyBytes(transport);

         // debug output
         int originalRead = bytes.readerIndex();
         byte[] frame = new byte[bytes.writerIndex()];
         bytes.getBytes(0, frame);
         try
         {
            System.err.println("Buffer outgoing: " + "\n" + ByteUtil.formatGroup(ByteUtil.bytesToHex(frame), 4, 16));
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         bytes.readerIndex(originalRead);
         // ^^ debug ouptut


         if (bytes != null)
         {
            // null means nothing to be written
            remotingConnection.write(new ChannelBufferWrapper(bytes));
         }
      }

      private ByteBuf getPooledNettyBytes(Transport transport)
      {
         int size = transport.pending();

         if (size == 0)
         {
            return null;
         }
//
         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(size);

         ByteBuffer bufferInput = transport.head();

         buffer.writeBytes(bufferInput);

         transport.pop(size);

         return buffer;
      }

   }

}
