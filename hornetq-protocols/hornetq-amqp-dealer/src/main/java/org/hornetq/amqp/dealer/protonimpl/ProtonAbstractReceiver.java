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
import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.amqp.dealer.exceptions.HornetQAMQPException;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         <p/>
 *         handles incoming messages via a Proton Receiver and forwards them to HornetQ
 */
public abstract class ProtonAbstractReceiver extends ProtonInitializable implements ProtonDeliveryHandler
{
   protected final ProtonAbstractConnectionImpl connection;

   protected final ProtonSessionImpl protonSession;

   protected final Receiver receiver;

   protected final String address;

   protected final ProtonSessionSPI sessionSPI;

   public ProtonAbstractReceiver(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, ProtonSessionImpl protonSession, Receiver receiver)
   {
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      if (receiver.getRemoteTarget() != null)
      {
         this.address = receiver.getRemoteTarget().getAddress();
      }
      else
      {
         this.address = null;
      }
      this.sessionSPI = sessionSPI;
   }

   protected int readDelivery(Receiver receiver, ByteBuf buffer)
   {
      int count;
      //todo an optimisation here would be to only use the buffer if we need more that one recv
      // optimization by norman
      while ((count = receiver.recv(buffer.array(), buffer.arrayOffset() + buffer.writerIndex(), buffer.writableBytes())) > 0)
      {
         // Increment the writer index by the number of bytes written into it while calling recv.
         buffer.writerIndex(buffer.writerIndex() + count);
      }
      return count;
   }

   @Override
   public void checkState()
   {
      //no op
   }

   @Override
   public void close() throws HornetQAMQPException
   {
      protonSession.removeReceiver(receiver);
   }

}
