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

package org.hornetq.core.protocol.proton.util;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportResultFactory;

/**
 * @author Clebert Suconic
 */

public abstract class ProtonTrio
{


   static ThreadLocal<Boolean> inDispatch = new ThreadLocal<>();
   private Sasl qpidServerSASL;

   protected final Transport transport = Proton.transport();

   protected final Connection connection = Proton.connection();

   protected final Collector collector = Proton.collector();

   protected final Object lock = new Object();

   private String username;

   private String password;

   private Executor executor;

   private boolean init = false;


   public ProtonTrio(Executor executor)
   {
      transport.bind(connection);
      connection.collect(collector);
      this.executor = executor;
   }


   Runnable dispatchRunnable = new Runnable()
   {
      public void run()
      {
         dispatch();
      }
   };

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }


   public Object getLock()
   {
      return lock;
   }

   private static final byte[] VERSION_HEADER = new byte[]{
      'A', 'M', 'Q', 'P', 0, 1, 0, 0
   };


   public void createServerSasl(String... mechanisms)
   {
      qpidServerSASL = transport.sasl();
      qpidServerSASL.server();
      qpidServerSASL.setMechanisms(mechanisms);
   }


   public void close()
   {
      synchronized (lock)
      {
         connection.close();
         transport.close();
         dispatch();
      }
   }

   public boolean pump(ByteBuf bytes)
   {
      try
      {
         synchronized (lock)
         {
            ByteBuffer tmp = bytes.internalNioBuffer(0, bytes.writerIndex());
            System.out.println("pos:" + tmp.position());
            transport.getInputBuffer().put(tmp);
            if (transport.processInput() != TransportResultFactory.ok())
            {
               connection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, "Error processing header"));
               qpidServerSASL = null;
               return false;
            }

            checkSASL();
            dispatch();

            return true;

         }
      }
      catch (Throwable e)
      {
         e.printStackTrace();
         return false;
      }
   }

   private void checkSASL()
   {
      if (qpidServerSASL != null && qpidServerSASL.getRemoteMechanisms().length > 0)
      {
         byte[] dataSASL = new byte[qpidServerSASL.pending()];
         qpidServerSASL.recv(dataSASL, 0, dataSASL.length);

         if (qpidServerSASL.getRemoteMechanisms()[0].equals("PLAIN"))
         {
            setUserPass(dataSASL);
         }

         // TODO: do the proper SASL authorization here
         // call an abstract method (authentication (bytes[])
         qpidServerSASL.done(Sasl.SaslOutcome.PN_SASL_OK);
         qpidServerSASL = null;
      }
   }

   public void dispatch()
   {
      synchronized (lock)
      {
         Event ev;
         while ((ev = collector.peek()) != null)
         {
            dispatch(ev);
            collector.pop();
         }

         // forcing transport on every dispatch call
         onTransport(transport);
      }
   }


   protected void onRemoteState(Connection connection)
   {
      if (connection.getRemoteState() == EndpointState.ACTIVE)
      {
         connection.open();
         connectionOpened(connection);
      }
      else if (connection.getRemoteState() == EndpointState.CLOSED)
      {
         System.out.println("Closing connection");
         connection.close();
         connectionClosed(connection);
      }
   }

   protected abstract void connectionOpened(Connection connection);

   protected abstract void connectionClosed(Connection connection);

   protected void onRemoteState(Session session)
   {
      if (session.getRemoteState() == EndpointState.ACTIVE)
      {
         session.open();
         sessionOpened(session);
      }
      else if (session.getRemoteState() == EndpointState.CLOSED)
      {
         session.close();
         sessionClosed(session);
      }
   }

   protected abstract void sessionOpened(Session session);

   protected abstract void sessionClosed(Session session);

   protected void onRemoteState(Link link)
   {
      if (link.getRemoteState() == EndpointState.ACTIVE)
      {
         link.open();
         linkOpened(link);
      }
      else if (link.getRemoteState() == EndpointState.CLOSED)
      {
         link.close();
         linkClosed(link);
      }
      else if (link.getLocalState() == EndpointState.ACTIVE)
      {
         linkActive(link);
      }
   }

   protected abstract void linkOpened(Link link);

   protected abstract void linkClosed(Link link);

   /**
    * Do we really need this? This used to be done with:
    * <p/>
    * <p/>
    * link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
    * while (link != null)
    * {
    * try
    * {
    * protonProtocolManager.handleActiveLink(link);
    * }
    * catch (HornetQAMQPException e)
    * {
    * link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
    * }
    * link = (LinkImpl) link.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
    * }
    */
   protected abstract void linkActive(Link link);

   protected void onLocalState(Connection connection)
   {
   }

   protected void onLocalState(Session session)
   {
   }

   protected void onLocalState(Link link)
   {
   }

   protected void onFlow(Link link)
   {

   }

   protected abstract void onDelivery(Delivery delivery);

   protected abstract void onTransport(Transport transport);

   private void dispatch(Event event)
   {

      if (inDispatch.get() != null)
      {
//         new Exception("Already in dispatch mode, using executor").printStackTrace();
         executor.execute(dispatchRunnable);
         return;
      }

      inDispatch.set(Boolean.TRUE);


      try
      {
         switch (event.getType())
         {
            case CONNECTION_REMOTE_STATE:
               onRemoteState(event.getConnection());
               break;
            case CONNECTION_LOCAL_STATE:
               onLocalState(event.getConnection());
               break;
            case SESSION_REMOTE_STATE:
               onRemoteState(event.getSession());
               break;
            case SESSION_LOCAL_STATE:
               onLocalState(event.getSession());
               break;
            case LINK_REMOTE_STATE:
               onRemoteState(event.getLink());
               break;
            case LINK_LOCAL_STATE:
               onLocalState(event.getLink());
               break;
            case LINK_FLOW:
               onFlow(event.getLink());
               break;
            case TRANSPORT:
               onTransport(event.getTransport());
               break;
            case DELIVERY:
               onDelivery(event.getDelivery());
               break;
         }
      }
      finally
      {
         inDispatch.set(null);
      }
   }


   /**
    * this is to be used with SASL
    */
   private void setUserPass(byte[] data)
   {
      String bytes = new String(data);
      String[] credentials = bytes.split(Character.toString((char) 0));
      int offSet = 0;
      if (credentials.length > 0)
      {
         if (credentials[0].length() == 0)
         {
            offSet = 1;
         }

         if (credentials.length >= offSet)
         {
            username = credentials[offSet];
         }
         if (credentials.length >= (offSet + 1))
         {
            password = credentials[offSet + 1];
         }
      }
   }


}
