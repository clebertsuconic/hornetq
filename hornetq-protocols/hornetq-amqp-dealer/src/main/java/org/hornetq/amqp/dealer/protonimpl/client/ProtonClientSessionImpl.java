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

package org.hornetq.amqp.dealer.protonimpl.client;

import org.apache.qpid.proton.engine.Session;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.dealer.protonimpl.ProtonAbstractConnectionImpl;
import org.hornetq.amqp.dealer.protonimpl.ProtonSessionImpl;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */

public class ProtonClientSessionImpl extends ProtonSessionImpl implements AMQPClientSession
{
   public ProtonClientSessionImpl(ProtonSessionSPI sessionSPI, ProtonAbstractConnectionImpl connection, Session session)
   {
      super(sessionSPI, connection, session);
   }

   public AMQPClientSender createSender(String address)
   {

      return null;
   }
}
