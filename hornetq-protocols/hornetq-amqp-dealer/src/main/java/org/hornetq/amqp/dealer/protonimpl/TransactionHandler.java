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

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.logger.HornetQAMQPProtocolMessageBundle;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * handles an amqp Coordinator to deal with transaction boundaries etc
 */
public class TransactionHandler implements ProtonDeliveryHandler
{

   final ProtonSessionSPI sessionSPI;

   public TransactionHandler(ProtonSessionSPI sessionSPI)
   {
      this.sessionSPI = sessionSPI;
   }

   @Override
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      ByteBuf buffer = sessionSPI.pooledBuffer(1024);

      final Receiver receiver;
      try
      {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable())
         {
            return;
         }

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
         MessageImpl msg = (MessageImpl) Message.Factory.create();
         msg.decode(bytes, 0, bytes.length);
         Object action = ((AmqpValue) msg.getBody()).getValue();

         if (action instanceof Declare)
         {
            Binary txID = sessionSPI.getCurrentTXID();
            Declared declared = new Declared();
            declared.setTxnId(txID);
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
                  sessionSPI.rollbackCurrentTX();
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
                  sessionSPI.commitCurrentTX();
               }
               catch (Exception e)
               {
                  throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCommittingCoordinator(e.getMessage());
               }
            }
            delivery.settle();
         }

      }
      catch (Exception e)
      {
         e.printStackTrace();
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();
         condition.setCondition(Symbol.valueOf("failed"));
         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         delivery.disposition(rejected);
      }
      finally
      {
         buffer.release();
      }
   }

   @Override
   public void checkState()
   {
      //noop
   }

   @Override
   public void close() throws HornetQAMQPException
   {
      //noop
   }
}
