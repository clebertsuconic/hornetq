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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.hornetq.common.example.HornetQExample;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends then receives a message.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new QueueExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {


      final int NUMBER = 1000;
      Thread[] threads = new Thread[NUMBER];

      for (int i = 0; i < NUMBER; i++)
      {
         threads[i] = new Thread("Thread " + i)
         {
            public void run()
            {
               try
               {
                  internalRun();
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         };
      }


      int i = 0;
      for (Thread t : threads)
      {
         if (i++ % 100 == 0)
         {
            //Thread.sleep(1000);
            System.out.println("Sleep...");
         }
         t.start();
      }


      while (true)
      {
         try
         {
            Thread.sleep(1000);
         }
         catch (Exception e)
         {
            e.printStackTrace();
            break;
         }
      }

      return true;

   }


   public boolean internalRun() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         MessageConsumer consumer = session.createConsumer(queue);

         connection.start();


         System.out.println("Receiving from Thread " + Thread.currentThread().getName());
         consumer.receive();


         return true;
      }
      finally
      {
         // Step 12. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}
