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
package org.hornetq.tests.performance.sends;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * Client-ack time
 *
 * @author Clebert Suconic
 */
public abstract class AbstractSendReceivePerfTest extends JMSTestBase
{
   protected static final String Q_NAME = "test-queue-01";
   private Queue queue;

   protected AtomicBoolean running = new AtomicBoolean(true);


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      jmsServer.createQueue(false, Q_NAME, null, true, Q_NAME);
      queue = HornetQJMSClient.createQueue(Q_NAME);

      AddressSettings settings = new AddressSettings();
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      settings.setMaxSizeBytes(Long.MAX_VALUE);
      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", settings);

   }


   @Override
   protected void registerConnectionFactory() throws Exception
   {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cf");

      cf = (ConnectionFactory) namingContext.lookup("/cf");
   }


   private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(AbstractSendReceivePerfTest.class.getName());


   @Test
   public void testSendReceive() throws Exception
   {
      long numberOfSamples = Long.getLong("HORNETQ_TEST_SAMPLES", 1000);


      MessageReceiver receiver = new MessageReceiver(Q_NAME, numberOfSamples);
      receiver.start();
      MessageSender sender = new MessageSender(Q_NAME);
      sender.start();

      receiver.join();
      sender.join();

      assertFalse(receiver.failed);
      assertFalse(sender.failed);

   }

   private class MessageReceiver extends Thread
   {
      private final String qName;
      private final long numberOfSamples;

      public boolean failed = false;

      public MessageReceiver(String qname, long numberOfSamples) throws Exception
      {
         super("Receiver " + qname);
         this.qName = qname;
         this.numberOfSamples = numberOfSamples;
      }

      @Override
      public void run()
      {
         try
         {
            LOGGER.info("Receiver: Connecting");
            Connection c = cf.createConnection();

            consumeMessages(c, qName);

            c.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            failed = true;
         }
         finally
         {
            running.set(false);
         }
      }
   }

   protected abstract void consumeMessages(Connection c, String qName) throws Exception;

   private class MessageSender extends Thread
   {
      protected String qName;

      public boolean failed = false;

      public MessageSender(String qname) throws Exception
      {
         super("Sender " + qname);

         this.qName = qname;
      }

      @Override
      public void run()
      {
         try
         {
            LOGGER.info("Sender: Connecting");
            Connection c = cf.createConnection();

            sendMessages(c, qName);

            c.close();

         }
         catch (Exception e)
         {
            failed = true;
            if (e instanceof InterruptedException)
            {
               LOGGER.info("Sender done.");
            }
            else
            {
               e.printStackTrace();
            }
         }
      }
   }

   /* This will by default send non persistent messages */
   protected void sendMessages(Connection c, String qName) throws JMSException
   {
      Session s = null;
      s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      LOGGER.info("Sender: Using AUTO-ACK session");


      Queue q = s.createQueue(qName);
      MessageProducer producer = s.createProducer(null);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);


      long sent = 0;
      while (running.get())
      {
         producer.send(q, s.createTextMessage("Message_" + (sent++)));
      }
   }
}