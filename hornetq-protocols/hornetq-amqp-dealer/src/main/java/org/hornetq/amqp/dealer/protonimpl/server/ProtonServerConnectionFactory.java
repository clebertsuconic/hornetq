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

import org.hornetq.amqp.dealer.AMQPConnection;
import org.hornetq.amqp.dealer.AMQPConnectionFactory;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;

/**
 * @author Clebert Suconic
 */

public class ProtonServerConnectionFactory extends AMQPConnectionFactory
{
   private static final AMQPConnectionFactory theInstance = new ProtonServerConnectionFactory();
   public static AMQPConnectionFactory getFactory()
   {
      return theInstance;
   }

   public AMQPConnection createConnection(ProtonConnectionSPI spi, boolean sasl)
   {
      ProtonServerConnectionImpl connection = new ProtonServerConnectionImpl(spi);
      if (sasl)
      {
         connection.createServerSASL();
      }
      return connection;
   }
}
