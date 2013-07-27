package org.hornetq.spi.core.remoting;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.client.impl.ClientLargeMessageInternal;
import org.hornetq.core.client.impl.ClientMessageInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;

/**
 * @author Clebert Suconic
 */

public abstract class SessionContext
{
   protected ClientSessionInternal session;


   public ClientSessionInternal getSession()
   {
      return session;
   }

   public void setSession(ClientSessionInternal session)
   {
      this.session = session;
   }


   /**
    * TODO: Move this to ConsumerContext
    * @param consumerID
    * @param clientLargeMessage
    * @param largeMessageSize
    * @throws Exception
    */
   protected void handleReceiveLargeMessage(long consumerID, ClientLargeMessageInternal clientLargeMessage, long largeMessageSize) throws Exception
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveLargeMessage(consumerID, clientLargeMessage, largeMessageSize);
      }
   }

   /**
    * TODO: Move this to ConsumerContext
    * @param consumerID
    * @param message
    * @throws Exception
    */
   protected void handleReceiveMessage(final long consumerID, final ClientMessageInternal message) throws Exception
   {

      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveMessage(consumerID, message);
      }
   }

   // TODO : move this to ConsumerContext
   protected void handleReceiveContinuation(final long consumerID, byte[] chunk, int flowControlSize, boolean isContinues) throws Exception
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveContinuation(consumerID, chunk, flowControlSize, isContinues);
      }
   }

   // TODO: move this to ProducerContext
   protected void handleReceiveProducerCredits(SimpleString address, int credits)
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveProducerCredits(address, credits);
      }

   }

   // TODO: move this to ProducerContext
   protected void handleReceiveProducerFailCredits(SimpleString address, int credits)
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveProducerFailCredits(address, credits);
      }

   }



}
