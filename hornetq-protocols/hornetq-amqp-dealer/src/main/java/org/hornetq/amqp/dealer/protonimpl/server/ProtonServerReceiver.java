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

package org.hornetq.amqp.dealer.protonimpl.server;

import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.logger.HornetQAMQPProtocolMessageBundle;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractReceiver;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */

public class ProtonServerReceiver extends ProtonAbstractReceiver
{
   public ProtonServerReceiver(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, ProtonSessionImpl protonSession, Receiver receiver)
   {
      super(sessionSPI, connection, protonSession, receiver);
   }


   @Override
   public void initialise() throws HornetQAMQPException
   {
      super.initialise();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      if (target != null)
      {
         if (target.getDynamic())
         {
            //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            String queue = sessionSPI.tempQueueName();


            sessionSPI.createTemporaryQueue(queue);
            target.setAddress(queue.toString());
         }
         else
         {
            //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
            //be a queue bound to it so we nee to check this.
            String address = target.getAddress();
            if (address == null)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.targetAddressNotSet();
            }
            try
            {
               if (!sessionSPI.queueQuery(address))
               {
                  throw HornetQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
               }
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorFindingTemporaryQueue(e.getMessage());
            }
         }
      }
   }

}
