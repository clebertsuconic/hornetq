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

package org.hornetq.amqp.test;

import org.hornetq.amqp.dealer.AMQPClientConnection;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.test.minimalclient.SimpleAMQPConnector;
import org.hornetq.amqp.test.minimalserver.MinimalServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class SimpleClientTest
{
   private MinimalServer server = new MinimalServer();

   @Before
   public void setUp() throws Exception
   {
      server.start("127.0.0.1", 5672, false);

   }

   @After
   public void tearDown() throws Exception
   {
      server.stop();
   }


   @Test
   public void testSimple() throws Exception
   {
      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnection clientConnection = connector.connect("127.0.0.1", 5672);

      clientConnection.clientOpen();

      AMQPClientSession session = clientConnection.createClientSession();
      AMQPClientSender clientSender = session.createSender("Test");



   }


}
