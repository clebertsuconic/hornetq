/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.tests.integration.persistence;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.xml.ws.Service;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

public class SendReceiveMultiThreadTest extends ServiceTestBase {

   ConnectionFactory cf;

   Destination destination;

   @Test
   public void testMultipleWrites() throws Exception {
      deleteDirectory(new File("./target/journaltmp"));
      HornetQServer server = createServer(true, true);
      server.getConfiguration().setJournalFileSize(10 * 1024 * 1024);
      server.getConfiguration().setJournalMinFiles(2);
      server.getConfiguration().setJournalType(JournalType.ASYNCIO);
      JMSServerManagerImpl serverManager = new JMSServerManagerImpl(server);
      server.getConfiguration().setJournalDirectory("./target/journaltmp/journal");
      server.getConfiguration().setBindingsDirectory("./target/journaltmp/bindings");
      server.getConfiguration().setPagingDirectory("./target/journaltmp/>paging");
      server.getConfiguration().setLargeMessagesDirectory("./target/journaltmp/largemessage");
      server.getConfiguration().setJournalMaxIO_AIO(200);

      serverManager.start();

      serverManager.createQueue(true, "performanceQueue", null, true);

      int NUMBER_OF_THREADS = 400;
      int NUMBER_OF_MESSAGES = 1000;

      MyThread[] threads = new MyThread[NUMBER_OF_THREADS];

      ConsumerThread[] cthreads = new ConsumerThread[NUMBER_OF_THREADS];

      final CountDownLatch alignFlag = new CountDownLatch(NUMBER_OF_THREADS);
      final CountDownLatch startFlag = new CountDownLatch(1);
      final CountDownLatch finishFlag = new CountDownLatch(NUMBER_OF_THREADS);

      cf = new HornetQJMSConnectionFactory(createNettyNonHALocator());

      destination = HornetQJMSClient.createQueue("performanceQueue");

      for (int i = 0; i < threads.length; i++) {
         threads[i] = new MyThread("sender::" + i, NUMBER_OF_MESSAGES, alignFlag, startFlag, finishFlag);
         cthreads[i] = new ConsumerThread(NUMBER_OF_MESSAGES);
      }

      for (ConsumerThread t : cthreads) {
         t.start();
      }

      for (MyThread t : threads) {
         t.start();
      }

      alignFlag.await();

      long startTime = System.currentTimeMillis();
      startFlag.countDown();

      // I'm using a countDown to avoid measuring time spent on thread context from join.
      // i.e. i want to measure as soon as the loops are done
      finishFlag.await();
      long endtime = System.currentTimeMillis();

      for (ConsumerThread t: cthreads) {
         t.join();
         Assert.assertEquals(0, t.errors);
      }

      long endTimeConsuming = System.currentTimeMillis();

      for (MyThread t : threads) {
         t.join();
         Assert.assertEquals(0, t.errors.get());
      }

      serverManager.stop();

      System.out.println("Time on sending:: " + (endtime - startTime));
      System.out.println("Time on consuming:: " + (endTimeConsuming - startTime));
   }

   AtomicInteger received = new AtomicInteger(0);

   class ConsumerThread extends Thread {

      final int numberOfMessages;

      Connection connection;
      Session session;

      MessageConsumer consumer;

      ConsumerThread(int numberOfMessages) throws Exception {
         super("consumerthread");
         this.numberOfMessages = numberOfMessages;

         connection = cf.createConnection();
         session = connection.createSession(true, Session.SESSION_TRANSACTED);
         consumer = session.createConsumer(destination);
         connection.start();
      }

      int errors = 0;

      public void run() {
         try {

            for (int i = 0; i < numberOfMessages; i++) {
               Message message = consumer.receive(50000);
               if (message == null) {
                  System.err.println("Could not receive message at i = " + numberOfMessages);
                  errors++;
                  break;
               }

               int r = received.incrementAndGet();

               if (r % 1000 == 0) {
                  System.out.println("Received " + r + " messages");
               }

               session.commit();
            }
            session.commit();
            connection.close();
         }
         catch (Exception e) {
            e.printStackTrace();
            errors++;
         }

      }
   }

   class MyThread extends Thread {

      final int numberOfMessages;
      final AtomicInteger errors = new AtomicInteger(0);

      final CountDownLatch align;
      final CountDownLatch start;
      final CountDownLatch finish;

      MyThread(String name, int numberOfMessages, CountDownLatch align, CountDownLatch start, CountDownLatch finish) {
         super(name);
         this.numberOfMessages = numberOfMessages;
         this.align = align;
         this.start = start;
         this.finish = finish;
      }

      public void run() {
         try {

            Connection connection = cf.createConnection();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            MessageProducer producer = session.createProducer(destination);

            align.countDown();
            start.await();

            for (int i = 0; i < numberOfMessages; i++) {
               BytesMessage msg = session.createBytesMessage();
               msg.writeBytes(new byte[1024]);
               producer.send(msg);
               session.commit();
            }

            connection.close();
            System.out.println("Send " + numberOfMessages + " messages on thread " + Thread.currentThread().getName());
         }
         catch (Exception e) {
            e.printStackTrace();
            errors.incrementAndGet();
         }
         finally {
            finish.countDown();
         }
      }
   }
}
