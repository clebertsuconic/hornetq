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
import java.nio.charset.StandardCharsets;

import io.netty.channel.ChannelPipeline;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.impl.LinkImpl;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPIllegalStateException;
import org.hornetq.core.remoting.impl.netty.NettyServerConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.utils.UUIDGenerator;

/**
 * A proton protocol manager, basically reads the Proton Input and maps proton resources to HornetQ resources
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonProtocolManager implements ProtocolManager, NotificationListener
{
   private final HornetQServer server;

   public ProtonProtocolManager(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection)
   {
      ProtonRemotingConnection conn = new ProtonRemotingConnection(acceptorUsed, connection, this, server.getExecutorFactory().getExecutor());
      //todo do we have a ttl?
      return new ConnectionEntry(conn, null, System.currentTimeMillis(), 1 * 60 * 1000);
   }

   @Override
   public void removeHandler(String name)
   {
   }

   @Override
   public void handleBuffer(RemotingConnection connection, HornetQBuffer buffer)
   {
//      // this is for debug only
      int index = buffer.readerIndex();
      byte[] frame = new byte[buffer.readableBytes()];
      buffer.readBytes(frame);

      try
      {
         System.err.println("Buffer Received: " + "\n" + ByteUtil.formatGroup(ByteUtil.bytesToHex(frame), 4, 16));
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      buffer.readerIndex(index);
//
//      // ^^^ Debug
//
      ProtonRemotingConnection protonRemotingConnection = (ProtonRemotingConnection) connection;
      protonRemotingConnection.setDataReceived();
      protonRemotingConnection.getTrio().pump(buffer.byteBuf());

   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeliner)
   {
      // KKK - TBD
//      pipeliner.addLast("amqp-decoder", new HornetQAMQPFrameDecoder());
   }

   @Override
   public boolean isProtocol(byte[] array)
   {
      String startFrame = new String(array, StandardCharsets.US_ASCII);
      return startFrame.startsWith("AMQP");
   }

   @Override
   public void handshake(NettyServerConnection connection, HornetQBuffer buffer)
   {
    // KKK - TBD
//      HornetQBuffer bufferHandshake = buffer.slice(0, 8);
//      connection.getProtocolConnection().bufferReceived(connection.getID(), buffer);
//      buffer.readerIndex(8);
   }

   @Override
   public void onNotification(Notification notification)
   {
      //noop
   }

   public ServerMessageImpl createServerMessage()
   {
      return new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
   }

   public void handleMessage(final Receiver receiver, HornetQBuffer buffer, final Delivery delivery,
                             final ProtonRemotingConnection connection, ProtonSession protonSession,
                             String address) throws Exception
   {
      synchronized (connection.getTrio().getLock())
      {
         int count;
         byte[] data = new byte[1024];
         //todo an optimisation here would be to only use the buffer if we need more that one recv
         while ((count = receiver.recv(data, 0, data.length)) > 0)
         {
            buffer.writeBytes(data, 0, count);
         }

         // we keep reading until we get end of messages, i.e. -1
         if (count == 0)
         {
            // todo this is obviously incorrect, investigate return;
         }
         receiver.advance();

         byte[] bytes = new byte[buffer.readableBytes()];
         buffer.readBytes(bytes);
         buffer.clear();
         EncodedMessage encodedMessage = new EncodedMessage(delivery.getMessageFormat(), bytes, 0, bytes.length);
         ServerMessage message = connection.getUtils().getInbound().transform(connection, encodedMessage);
         //use the address on the receiver if not null, if null let's hope it was set correctly on the message
         if (address != null)
         {
            message.setAddress(new SimpleString(address));
         }
         //todo decide on whether to deliver direct
         protonSession.getServerSession().send(message, true);

         receiver.flow(1);
         delivery.settle();

//         server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
//         {
//            @Override
//            public void done()
//            {
//               synchronized (connection.getTrio().getLock())
//               {
//                  receiver.flow(1);
//                  delivery.settle();
//                  connection.getTrio().dispatch();
//               }
//            }
//
//            @Override
//            public void onError(int errorCode, String errorMessage)
//            {
//               receiver.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, errorCode + ":" + errorMessage));
//            }
//         });
      }
   }

   public void handleDelivery(final Sender sender, final byte[] tag, final EncodedMessage encodedMessage, final ServerMessage message, final ProtonRemotingConnection connection, final boolean preSettle)
   {
      synchronized (connection.getTrio().getLock())
      {
         final Delivery delivery;
         delivery = sender.delivery(tag, 0, tag.length);
         delivery.setContext(message);
         sender.send(encodedMessage.getArray(), 0, encodedMessage.getLength());

         ((LinkImpl) sender).addCredit(1);

         if (preSettle)
         {
            delivery.settle();
         }
         else
         {
            sender.advance();
         }

         connection.getTrio().dispatch();
      }
//      server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
//      {
//         @Override
//         public void done()
//         {
//            synchronized (connection.getTrio().getLock())
//            {
//               final Delivery delivery;
//               delivery = sender.delivery(tag, 0, tag.length);
//               delivery.setContext(message);
//               sender.send(encodedMessage.getArray(), 0, encodedMessage.getLength());
//
//               ((LinkImpl) sender).addCredit(1);
//
//               if (preSettle)
//               {
//                  delivery.settle();
//               }
//               else
//               {
//                  sender.advance();
//               }
//
//               connection.getTrio().dispatch();
//            }
//         }
//
//         @Override
//         public void onError(int errorCode, String errorMessage)
//         {
//            sender.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, errorCode + ":" + errorMessage));
//         }
//      });
   }

   void handleNewLink(Link link, ProtonSession protonSession, ProtonRemotingConnection protonConnection) throws HornetQAMQPException
   {
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
            protonSession.addProducer(receiver);
            //todo do this using the server session flow control
            receiver.flow(100);
         }
      }
      else
      {

         synchronized (protonConnection.getTrio().getLock())
         {
            protonSession.initialise(false);
            Sender sender = (Sender) link;
            protonSession.addConsumer(sender);
            sender.offer(1);
         }
      }
   }

   public ProtonSession createSession(ProtonRemotingConnection protonConnection) throws HornetQAMQPException
   {
      String name = UUIDGenerator.getInstance().generateStringUUID();
      return new ProtonSession(name, protonConnection, this, server.getStorageManager()
         .newContext(server.getExecutorFactory().getExecutor()), server);
   }

   void handleActiveLink(Link link) throws HornetQAMQPException
   {
      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      ProtonDeliveryHandler handler = (ProtonDeliveryHandler) link.getContext();
      handler.checkState();
   }

   public void handleTransaction(Receiver receiver, HornetQBuffer buffer, Delivery delivery, ProtonSession protonSession) throws HornetQAMQPIllegalStateException
   {
      int count;
      byte[] data = new byte[1024];
      //todo an optimisation here would be to only use the buffer if we need more that one recv
      while ((count = receiver.recv(data, 0, data.length)) > 0)
      {
         buffer.writeBytes(data, 0, count);
      }

      // we keep reading until we get end of messages, i.e. -1
      if (count == 0)
      {
         return;
      }
      receiver.advance();
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes);
      buffer.clear();
      MessageImpl msg = new MessageImpl();
      msg.decode(bytes, 0, bytes.length);
      Object action = ((AmqpValue) msg.getBody()).getValue();
      if (action instanceof Declare)
      {
         Transaction tx = protonSession.getServerSession().getCurrentTransaction();
         Declared declared = new Declared();
         declared.setTxnId(new Binary(longToBytes(tx.getID())));
         delivery.disposition(declared);
         delivery.settle();
      }
      else if (action instanceof Discharge)
      {
         Discharge discharge = (Discharge) action;
         if (discharge.getFail())
         {
            try
            {
               protonSession.getServerSession().rollback(false);
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorRollingbackCoordinator(e.getMessage());
            }
         }
         else
         {
            try
            {
               protonSession.getServerSession().commit();
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCommittingCoordinator(e.getMessage());
            }
         }
         delivery.settle();
      }
   }

   public byte[] longToBytes(long x)
   {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      buffer.putLong(x);
      return buffer.array();
   }
}
