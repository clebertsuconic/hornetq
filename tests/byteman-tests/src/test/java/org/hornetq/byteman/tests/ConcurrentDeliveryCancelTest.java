/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.byteman.tests;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.tests.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentDeliveryCancelTest extends JMSTestBase
{

   @Test
   public void testConcurrentCancels() throws Exception
   {

      server.getAddressSettingsRepository().clear();
      AddressSettings settings = new AddressSettings();
      settings.setMaxDeliveryAttempts(-1);
      server.getAddressSettingsRepository().addMatch("#", settings);
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF, new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
      cf.setReconnectAttempts(0);
      cf.setRetryInterval(10);


      System.out.println(".....");
      for (ServerSession srvSess : server.getSessions())
      {
         System.out.println(srvSess);
      }

      String queueName = RandomUtil.randomString();
      Queue queue = createQueue(queueName);

      Connection connection = cf.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(queue);

      int numberOfMessages = 1000;
      for (int i = 0; i < numberOfMessages; i++)
      {
         TextMessage msg = session.createTextMessage("message " + i);
         msg.setIntProperty("i", i);
         producer.send(msg);
      }
      session.commit();

      connection.close();

      for (int i = 0; i < 100; i++)
      {
         connection = cf.createConnection();
         connection.setClientID("myID");

         session = connection.createSession(true, Session.SESSION_TRANSACTED);

         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);

         Assert.assertNotNull(consumer.receiveNoWait());

         System.out.println(".....");

         final List<ServerSession> serverSessions = new LinkedList<ServerSession>();

         // We will force now the failure simultaneously from several places
         for (ServerSession srvSess : server.getSessions())
         {
            System.out.println(srvSess);
            serverSessions.add(srvSess);
         }


         int NTHREADS = 10;
         final CountDownLatch alignLatch = new CountDownLatch(NTHREADS);
         final CountDownLatch flagStart = new CountDownLatch(1);

         List<Thread> threads = new LinkedList<Thread>();

         for (int i1 = 0; i1 < NTHREADS; i1++)
         {
            threads.add(new Thread()
            {
               public void run()
               {
                  alignLatch.countDown();
                  for (int i = 0; i < 2; i++)
                  {
                     for (ServerSession sess : serverSessions)
                     {
                        sess.getRemotingConnection().fail(new HornetQException("failure"));
                     }
                  }
               }
            });
         }

         for (Thread t : threads)
         {
            t.start();
         }

         alignLatch.await();
         flagStart.countDown();

         ClientSessionInternal impl = (ClientSessionInternal) ((HornetQSession)session).getCoreSession();
         impl.getConnection().fail(new HornetQException("failure"));
         connection.close();

         for (Thread t: threads)
         {
            t.join();
         }
      }

      connection = cf.createConnection();
      connection.setClientID("myID");

      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queue);
      HashMap<Integer, AtomicInteger> mapCount = new HashMap<Integer, AtomicInteger>();

      while (true)
      {
         TextMessage message = (TextMessage)consumer.receiveNoWait();
         if (message == null)
         {
            break;
         }

         Integer value = message.getIntProperty("i");

         System.out.println("received message = " + value);

         AtomicInteger count = mapCount.get(value);
         if (count == null)
         {
            count = new AtomicInteger(0);
            mapCount.put(message.getIntProperty("i"), count);
         }
         else
         {
            System.out.println("Duplicated message");
         }

         count.incrementAndGet();
      }

      boolean failed = false;
      for (int i = 0; i < numberOfMessages; i++)
      {
         AtomicInteger count = mapCount.get(i);
         if (count == null)
         {
            System.out.println("Message " + i + " not received");
            failed = true;
         }
         else if (count.get() > 1)
         {
            System.out.println("Message " + i + " received " + count.get() + " times");
            failed = true;
         }
      }

      Assert.assertFalse("test failed, look at the system.out of the test for more infomration", failed);

      connection.close();


   }
}
