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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPInternalErrorException;
import org.hornetq.amqp.dealer.logger.HornetQAMQPProtocolMessageBundle;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/10/13
 */
public abstract class ProtonSessionImpl extends ProtonInitializable
{
   protected final ProtonAbstractConnectionImpl connection;

   protected final ProtonSessionSPI sessionSPI;

   protected final Session session;

   private long currentTag = 0;

   protected Map<Receiver, ProtonAbstractReceiver> receivers = new HashMap<Receiver, ProtonAbstractReceiver>();

   protected Map<Sender, AbstractProtonSender> senders = new HashMap<Sender, AbstractProtonSender>();

   protected boolean closed = false;

   public ProtonSessionImpl(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, Session session)
   {
      this.connection = connection;
      this.sessionSPI = sessionSPI;
      this.session = session;
   }

   /*
   * we need to setTransacted the actual server session when we receive the first linkas this tells us whether or not the
   * session is transactional
   * */
   public void setTransacted(boolean transacted) throws HornetQAMQPInternalErrorException
   {
      try
      {
         sessionSPI.init(this, connection.getLogin(), connection.getPasscode(), transacted);
      }
      catch (Exception e)
      {
         throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCreatingHornetQSession(e.getMessage());
      }
   }

   public void disconnect(Object consumer, String queueName)
   {
      AbstractProtonSender protonConsumer = senders.remove(consumer);
      if (protonConsumer != null)
      {
         try
         {
            protonConsumer.close();
         }
         catch (HornetQAMQPException e)
         {
            protonConsumer.getSender().setTarget(null);
            protonConsumer.getSender().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
      }
   }


   public byte[] getTag()
   {
      return Long.toHexString(currentTag++).getBytes();
   }

   public void replaceTag(byte[] tag)
   {
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      for (ProtonAbstractReceiver protonProducer : receivers.values())
      {
         try
         {
            protonProducer.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            // TODO Logging
         }
      }
      receivers.clear();
      for (AbstractProtonSender protonConsumer : senders.values())
      {
         try
         {
            protonConsumer.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            // TODO Logging
         }
      }
      senders.clear();
      try
      {
         sessionSPI.rollbackCurrentTX();
         sessionSPI.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         // TODO logging
      }
      closed = true;
   }

   public void removeSender(Sender sender) throws HornetQAMQPException
   {
      senders.remove(sender);
   }

   public void removeReceiver(Receiver receiver)
   {
      receivers.remove(receiver);
   }
}
