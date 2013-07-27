/*
 * Copyright 2013 Red Hat, Inc.
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

package org.hornetq.core.protocol.core.impl;

import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.impl.ClientLargeMessageInternal;
import org.hornetq.core.client.impl.ClientMessageInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsFailMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.spi.core.remoting.SessionContext;

import static org.hornetq.core.protocol.core.impl.PacketImpl.EXCEPTION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_CONTINUATION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_LARGE_MSG;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_MSG;

/**
 * @author Clebert Suconic
 */

public class HornetQSessionContext extends SessionContext
{
   private final Channel sessionChannel;
   private final int serverVersion;


   public HornetQSessionContext(Channel sessionChannel, int serverVersion)
   {
      this.sessionChannel = sessionChannel;
      this.serverVersion = serverVersion;

      ChannelHandler handler = new ClientSessionPacketHandler();
      sessionChannel.setHandler(handler);

   }

   public int getServerVersion()
   {
      return serverVersion;
   }

   public Channel getSessionChannel()
   {
      return sessionChannel;
   }


   private void handleReceivedMessagePacket(SessionReceiveMessage messagePacket) throws Exception
   {
      ClientMessageInternal msgi = (ClientMessageInternal)messagePacket.getMessage();

      msgi.setDeliveryCount(messagePacket.getDeliveryCount());

      msgi.setFlowControlSize(messagePacket.getPacketSize());

      handleReceiveMessage(messagePacket.getConsumerID(), msgi);
   }



   private void handleReceiveLargeMessage(SessionReceiveLargeMessage serverPacket) throws Exception
   {
      ClientLargeMessageInternal clientLargeMessage = (ClientLargeMessageInternal)serverPacket.getLargeMessage();

      clientLargeMessage.setFlowControlSize(serverPacket.getPacketSize());

      clientLargeMessage.setDeliveryCount(serverPacket.getDeliveryCount());

      handleReceiveLargeMessage(serverPacket.getConsumerID(), clientLargeMessage, serverPacket.getLargeMessageSize());
   }


   private void handleReceiveContinuation(SessionReceiveContinuationMessage continuationPacket) throws Exception
   {
      handleReceiveContinuation(continuationPacket.getConsumerID(), continuationPacket.getBody(), continuationPacket.getPacketSize(),
                                continuationPacket.isContinues());
   }


   protected void handleReceiveProducerCredits(SessionProducerCreditsMessage message)
   {
      handleReceiveProducerCredits(message.getAddress(), message.getCredits());
   }


   protected void handleReceiveProducerFailCredits(SessionProducerCreditsFailMessage message)
   {
      handleReceiveProducerFailCredits(message.getAddress(), message.getCredits());
   }

   class ClientSessionPacketHandler implements ChannelHandler
   {

      public void handlePacket(final Packet packet)
      {
         byte type = packet.getType();

         try
         {
            switch (type)
            {
               case SESS_RECEIVE_CONTINUATION:
               {
                  handleReceiveContinuation((SessionReceiveContinuationMessage)packet);

                  break;
               }
               case SESS_RECEIVE_MSG:
               {
                  handleReceivedMessagePacket((SessionReceiveMessage) packet);

                  break;
               }
               case SESS_RECEIVE_LARGE_MSG:
               {
                  handleReceiveLargeMessage((SessionReceiveLargeMessage)packet);

                  break;
               }
               case PacketImpl.SESS_PRODUCER_CREDITS:
               {
                  handleReceiveProducerCredits((SessionProducerCreditsMessage) packet);

                  break;
               }
               case PacketImpl.SESS_PRODUCER_FAIL_CREDITS:
               {
                  handleReceiveProducerFailCredits((SessionProducerCreditsFailMessage) packet);

                  break;
               }
               case EXCEPTION:
               {
                  // We can only log these exceptions
                  // maybe we should cache it on SessionContext and throw an exception on any next calls
                  HornetQExceptionMessage mem = (HornetQExceptionMessage)packet;

                  HornetQClientLogger.LOGGER.receivedExceptionAsynchronously(mem.getException());

                  break;
               }
               default:
               {
                  throw new IllegalStateException("Invalid packet: " + type);
               }
            }
         }
         catch (Exception e)
         {
            HornetQClientLogger.LOGGER.failedToHandlePacket(e);
         }

         sessionChannel.confirm(packet);
      }
   }


}
