/*
 * Copyright 2009 Red Hat, Inc.
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
package org.hornetq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.common.example.HornetQExample;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.jms.client.HornetQConnectionFactory;

/**
 * This example demonstrates how sessions created from a single connection can be load
 * balanced across the different nodes of the cluster.
 *
 * In this example there are three nodes and we use a round-robin client side load-balancing
 * policy.
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class ClientSideLoadBalancingExample extends HornetQExample {


   CyclicBarrier barrier = new CyclicBarrier(100);

   public static void main(final String[] args) {
      new ClientSideLoadBalancingExample().run(args);
   }

   static int tcount = 0;

   class MyRunner extends Thread {

      MyRunner() {
         super("Thread " + (++tcount));
      }

      @Override
      public void run() {
         try {
            long i = 0;

            while (true) {

               DiscoveryGroupConfiguration groupConfiguration =
                  new DiscoveryGroupConfiguration(HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                                                  new UDPBroadcastGroupConfiguration("231.7.7.7", 9876, null, -1));

               HornetQConnectionFactory connectionFactory = new HornetQConnectionFactory(true, groupConfiguration);


               // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
               ServerLocatorImpl locator = (ServerLocatorImpl) ((HornetQConnectionFactory) connectionFactory).getServerLocator();
               // locator.setInitialConnectAttempts(10);
               // locator.initialize();
               // locator.getDiscoveryGroup().stop();

               if (i % 10 == 0) System.out.println(Thread.currentThread().getName() + " is running a connection at " + i);
               Connection connection = connectionFactory.createConnection();
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               connection.close();

               i++;

               ((HornetQConnectionFactory) connectionFactory).close();
            }

         } catch (Exception e) {
            for (int i = 0; i < 10; i++) {
               e.printStackTrace(System.out);
            }
            System.out.flush();
            System.exit(-1);
         }
      }
   }

   @Override
   public boolean runExample() throws Exception
   {
      for (int i = 0; i < 100; i++) {
         Thread t = new MyRunner();
         t.start();
      }

      while (true) {
         Thread.sleep(100);

         Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();

         for (Thread t : threads.keySet()) {
            if (t.getName().startsWith("activemq-discovery-group-thread-")) {
               t.interrupt();
            }
         }
      }
   }
}
