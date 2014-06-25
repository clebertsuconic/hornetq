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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.amqp.dealer.AMQPClientConnection;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.dealer.SASLPlain;
import org.hornetq.amqp.test.minimalclient.SimpleAMQPConnector;
import org.hornetq.amqp.test.minimalserver.DumbServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@RunWith(Parameterized.class)
public class ProtonTest extends AbstractJMSTest
{

   protected Connection connection;

   @Parameterized.Parameters(name = "useHawt={0} sasl={1}")
   public static Collection<Object[]> data()
   {
      List<Object[]> list = Arrays.asList(new Object[][]{
         {Boolean.TRUE, Boolean.TRUE},
         {Boolean.FALSE, Boolean.TRUE}});

      System.out.println("Size = " + list.size());
      return list;
   }

   public ProtonTest(boolean useHawtJMS, boolean useSASL)
   {
      super(useHawtJMS, useSASL);
   }


   @Before
   public void setUp() throws Exception
   {
      DumbServer.clear();
      server.start("127.0.0.1", 5672, useSASL);
      connection = createConnection();

   }

   @After
   public void tearDown() throws Exception
   {
      if (connection != null)
      {
         connection.close();
      }

      super.tearDown();
   }

   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      final int numMessages = getNumberOfMessages();
      long time = System.currentTimeMillis();
      final Queue queue = createQueue();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            Connection connectionConsumer = null;
            try
            {
               connectionConsumer = createConnection();
//               connectionConsumer = connection;
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

               int count = numMessages;
               while (count > 0)
               {
                  try
                  {
                     BytesMessage m = (BytesMessage) consumer.receive(1000);
                     if (count % 1000 == 0)
                     {
                        System.out.println("Count = " + count + ", property=" + m.getStringProperty("XX"));
                     }
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

      Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

      t.start();

      MessageProducer p = session.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.PERSISTENT);
      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage message = session.createBytesMessage();
         // TODO: this will break stuff if I use a large number
         message.writeBytes(new byte[5]);
         message.setIntProperty("count", i);
         message.setStringProperty("XX", "count" + i);
         p.send(message);
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

      connection.close();
//      assertEquals(0, q.getMessageCount());
   }

   @Test
   public void testMapMessage() throws Exception
   {
      Queue queue = createQueue();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < 10; i++)
      {
         MapMessage message = session.createMapMessage();
         message.setInt("x", i);
         message.setString("str", "str" + i);
         p.send(message);
      }
      MessageConsumer messageConsumer = session.createConsumer(queue);
      for (int i = 0; i < 10; i++)
      {
         MapMessage m = (MapMessage) messageConsumer.receive(5000);
         Assert.assertNotNull(m);
         Assert.assertEquals(i, m.getInt("x"));
         Assert.assertEquals("str" + i, m.getString("str"));
      }

      Assert.assertNull(messageConsumer.receiveNoWait());
   }

   @Test
   public void testProperties() throws Exception
   {
      Queue queue = createQueue();
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

   @Test
   public void testSendWithSimpleClient() throws Exception
   {
      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnection clientConnection = connector.connect("127.0.0.1", 5672);

      clientConnection.clientOpen(new SASLPlain("aa", "aa"));

      AMQPClientSession session = clientConnection.createClientSession();
      AMQPClientSender clientSender = session.createSender(address, true);


      Properties props = new Properties();
      for (int i = 0; i < 1; i++)
      {
         MessageImpl message = (MessageImpl) Message.Factory.create();

         HashMap map = new HashMap();

         map.put("i", i);
         AmqpValue value = new AmqpValue(map);
         message.setBody(value);
         message.setProperties(props);
         clientSender.send(message);
      }

      Session clientSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      MessageConsumer consumer = clientSession.createConsumer(createQueue());
      for (int i = 0; i < 1; i++)
      {
         MapMessage msg = (MapMessage) consumer.receive(5000);
         System.out.println("Msg " + msg);
         Assert.assertNotNull(msg);

         System.out.println("Receive message " + i);

         Assert.assertEquals(0, msg.getInt("i"));
      }
   }


   protected int getNumberOfMessages()
   {
      return 10000;
   }

}
