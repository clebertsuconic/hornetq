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

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.amqp.dealer.AMQPClientConnection;
import org.hornetq.amqp.dealer.AMQPClientReceiver;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.dealer.SASLPlain;
import org.hornetq.amqp.test.minimalclient.SimpleAMQPConnector;
import org.hornetq.amqp.test.minimalserver.MinimalServer;
import org.junit.After;
import org.junit.Assert;
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
      server.start("127.0.0.1", 5672, true);

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

      clientConnection.clientOpen(new SASLPlain("aa", "aa"));

      AMQPClientSession session = clientConnection.createClientSession();
      AMQPClientSender clientSender = session.createSender("Test", true);
      Properties props = new Properties();

      MessageImpl message = (MessageImpl) Message.Factory.create();

      Data value = new Data(new Binary(new byte[5]));

      message.setBody(value);
      clientSender.send(message);

      AMQPClientReceiver receiver = session.createReceiver("Test");

      receiver.flow(1000);

      message = (MessageImpl) receiver.receiveMessage(5, TimeUnit.SECONDS);

      System.out.println("Received message " + message.getBody());


   }

   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      SimpleAMQPConnector connector1 = new SimpleAMQPConnector();
      connector1.start();
      final AMQPClientConnection clientConnection = connector1.connect("127.0.0.1", 5672);
      clientConnection.clientOpen(new SASLPlain("AA", "AA"));


      SimpleAMQPConnector connector2 = new SimpleAMQPConnector();
      connector2.start();
      final AMQPClientConnection connectionConsumer = connector2.connect("127.0.0.1", 5672);
      connectionConsumer.clientOpen(new SASLPlain("AA", "AA"));


      final int numMessages = getNumberOfMessages();
      long time = System.currentTimeMillis();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               AMQPClientSession sessionConsumer = connectionConsumer.createClientSession();
               AMQPClientReceiver receiver = sessionConsumer.createReceiver("Test");
               receiver.flow(getNumberOfMessages() * 2);

               int received = 0;
               int count = numMessages;
               while (count > 0)
               {
                  received++;
                  if (received % 500 == 0)
                  {
                     System.out.println("Received " + received);
                  }

                  try
                  {
                     MessageImpl m = (MessageImpl) receiver.receiveMessage(5, TimeUnit.SECONDS);
                     Assert.assertNotNull("Could not receive message count=" + count + " on consumer", m);
                     count--;
                  }
                  catch (JMSException e)
                  {
                     break;
                  }
               }
            }
            catch (Throwable e)
            {
               exceptions.add(e);
               e.printStackTrace();
            }
         }
      });

      AMQPClientSession session = clientConnection.createClientSession();

      t.start();

      AMQPClientSender sender = session.createSender("Test", true);
      for (int i = 0; i < numMessages; i++)
      {
         MessageImpl message = (MessageImpl) Message.Factory.create();
         message.setBody(new Data(new Binary(new byte[5])));
         sender.send(message);
      }

      long taken = (System.currentTimeMillis() - time);
      System.out.println("taken on send = " + taken);
      t.join();

      for (Throwable e : exceptions)
      {
         throw e;
      }
      taken = (System.currentTimeMillis() - time);
      System.out.println("taken = " + taken);

   }


   protected int getNumberOfMessages()
   {
      return 5000;
   }

}
