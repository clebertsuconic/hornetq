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

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.hornetq.amqp.test.dumbserver.MinimalServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonTest
{
   private String address = "exampleQueue";
   private Connection connection;

   private MinimalServer server = new MinimalServer();

   @Before
   public void setUp() throws Exception
   {
      server.start("127.0.0.1", 5672);
      connection = createConnection();

   }

   @After
   public void tearDown() throws Exception
   {
      if (connection != null)
      {
         connection.close();
      }

      server.stop();
   }


   /*
   // Uncomment testLoopBrowser to validate the hunging on the test
   @Test
   public void testLoopBrowser() throws Throwable
   {
      for (int i = 0 ; i < 1000; i++)
      {
         System.out.println("#test " + i);
         testBrowser();
         tearDown();
         setUp();
      }
   } */

   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      final int numMessages = 10;
      long time = System.currentTimeMillis();
      final QueueImpl queue = new QueueImpl(address);

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            Connection connectionConsumer = null;
            try
            {
//               connectionConsumer = createConnection();
               connectionConsumer = connection;
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

               int count = numMessages;
               while (count > 0)
               {
                  try
                  {
                     Message m = consumer.receive(5000);
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
            finally
            {
               try
               {
                  // if the createconnecion wasn't commented out
                  if (connectionConsumer != connection)
                  {
                     connectionConsumer.close();
                  }
               }
               catch (Throwable ignored)
               {
                  // NO OP
               }
            }
         }
      });

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         message.setIntProperty("count", i);
         p.send(message);
      }

      t.start();
      t.join();

      for (Throwable e : exceptions)
      {
         throw e;
      }

      connection.close();
//      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }


   @Test
   public void testProperties() throws Exception
   {
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      message.setBooleanProperty("true", true);
      message.setBooleanProperty("false", false);
      message.setStringProperty("foo", "bar");
      message.setDoubleProperty("double", 66.6);
      message.setFloatProperty("float", 56.789f);
      message.setIntProperty("int", 8);
      message.setByteProperty("byte", (byte) 10);
      p.send(message);
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue);
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals("msg:0", m.getText());
      Assert.assertEquals(m.getBooleanProperty("true"), true);
      Assert.assertEquals(m.getBooleanProperty("false"), false);
      Assert.assertEquals(m.getStringProperty("foo"), "bar");
      Assert.assertEquals(m.getDoubleProperty("double"), 66.6, 0.0001);
      Assert.assertEquals(m.getFloatProperty("float"), 56.789f, 0.0001);
      Assert.assertEquals(m.getIntProperty("int"), 8);
      Assert.assertEquals(m.getByteProperty("byte"), (byte) 10);
      m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      connection.close();
   }

   private javax.jms.Connection createConnection() throws JMSException
   {
      final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", 5672, "bbb", "aaa");
      final javax.jms.Connection connection = factory.createConnection();
      connection.setExceptionListener(new ExceptionListener()
      {
         @Override
         public void onException(JMSException exception)
         {
            exception.printStackTrace();
         }
      });
      connection.start();
      return connection;
   }
}
