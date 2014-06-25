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
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;

import io.hawtjms.jms.JmsConnectionFactory;
import io.hawtjms.jms.JmsQueue;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.hornetq.amqp.test.minimalserver.DumbServer;
import org.hornetq.amqp.test.minimalserver.MinimalServer;

/**
 * @author Clebert Suconic
 */

public class AbstractJMSTest
{
   protected final boolean useHawtJMS;
   protected final boolean useSASL;

   protected String address = "exampleQueue";
   protected MinimalServer server = new MinimalServer();

   public AbstractJMSTest(boolean useHawtJMS, boolean useSASL)
   {
      this.useHawtJMS = useHawtJMS;
      this.useSASL = useSASL;
   }

   public void tearDown() throws Exception
   {
      server.stop();
      DumbServer.clear();
   }


   protected Connection createConnection() throws JMSException
   {
      final ConnectionFactory factory = createConnectionFactory();
      final Connection connection = factory.createConnection();
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


   protected ConnectionFactory createConnectionFactory()
   {
      if (useSASL)
      {
         if (useHawtJMS)
         {
            return new JmsConnectionFactory("aaaaaaaa", "aaaaaaa", "amqp://localhost:5672");
         }
         else
         {
            return new ConnectionFactoryImpl("localhost", 5672, "aaaaaaaa", "aaaaaaa");
         }
      }
      else
      {
         if (useHawtJMS)
         {
            return new JmsConnectionFactory("amqp://localhost:5672");
         }
         else
         {
            return new ConnectionFactoryImpl("localhost", 5672, "aaaaaaaa", "aaaaaaa");
         }

      }
   }

   protected Queue createQueue()
   {
      if (useHawtJMS)
      {
         return new JmsQueue(address);
      }
      else
      {
         return new QueueImpl(address);
      }
   }


}
