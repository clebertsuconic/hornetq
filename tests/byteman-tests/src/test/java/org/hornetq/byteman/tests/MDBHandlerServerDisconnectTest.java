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
package org.hornetq.byteman.tests;

import javax.jms.Message;
import javax.resource.ResourceException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.hornetq.api.core.HornetQConnectionTimedOutException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.integration.ra.HornetQRATestBase;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author mnovak
 */
@RunWith(BMUnitRunner.class)
public class MDBHandlerServerDisconnectTest extends HornetQRATestBase
{
   final ConcurrentHashMap<Integer, AtomicInteger> mapCounter = new ConcurrentHashMap<Integer, AtomicInteger>();

   final AtomicBoolean running = new AtomicBoolean(true);

   Transaction currentTX;

   volatile HornetQResourceAdapter resourceAdapter;


   @Before
   public void setUp() throws Exception
   {
      mapCounter.clear();
      running.set(true);
      resourceAdapter = null;
      super.setUp();
      createQueue(true, "outQueue");
      DummyTMLocator.startTM();
   }

   @After
   public void tearDown() throws Exception
   {
      DummyTMLocator.stopTM();
      super.tearDown();
   }

   protected boolean usePersistence()
   {
      return true;
   }

   @Override
   public boolean useSecurity()
   {
      return false;
   }

   @Test
   public void testSimpleMessageReceivedOnQueueTwoPhaseFailPrepareByConnectionTimout() throws Exception
   {
      AddressSettings settings = new AddressSettings();
      settings.setRedeliveryDelay(500);
      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", settings);
      HornetQResourceAdapter qResourceAdapter = newResourceAdapter();
      resourceAdapter = qResourceAdapter;

      qResourceAdapter.setTransactionManagerLocatorClass(DummyTMLocator.class.getName());
      qResourceAdapter.setTransactionManagerLocatorMethod("getTM");

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      qResourceAdapter.start(ctx);

      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setMaxSession(1);
      spec.setTransactionTimeout(1);
      spec.setReconnectAttempts(-1);
      spec.setCallTimeout(1000L);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setConsumerWindowSize(1024 * 1024);

      XADummyEndpoint endpoint = new XADummyEndpoint();

      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, true);

      qResourceAdapter.endpointActivation(endpointFactory, spec);

      ClientSession session = locator.createSessionFactory().createSession();



      List<Thread> threads = new ArrayList<Thread>();

      for (int i = 0; i < 100; i++)
      {
         Thread myThread = new MyThread("consumer " + i);
         myThread.start();
         threads.add(myThread);
      }

      Thread.sleep(1000);

      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);

      final int NUMBER_OF_MESSAGES = 2000;

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {

         ClientMessage message = session.createMessage(true);

         message.getBodyBuffer().writeString("teststring " + i);
         message.putIntProperty("i", i);

         clientProducer.send(message);
      }
      session.commit();
      session.close();

      Assert.assertTrue(endpoint.latchEnter.await(2, TimeUnit.SECONDS));

      Assert.assertEquals(1, resourceAdapter.getActivations().values().size());

      HornetQActivation activation = resourceAdapter.getActivations().values().toArray(new HornetQActivation[1])[0];

      Assert.assertEquals(1, activation.getHandlers().size());

      ClientSessionFactoryImpl factory = (ClientSessionFactoryImpl)activation.getHandlers().get(0).getCf();

      Thread.sleep(1000);

      factory.getConnection().fail(new HornetQConnectionTimedOutException("failure"));

      endpoint.latchWait.countDown();

      qResourceAdapter.stop();


      running.set(false);

      for (Thread thread : threads)
      {
         thread.join();
      }

      boolean failed = false;
      for (Map.Entry<Integer, AtomicInteger> entry: mapCounter.entrySet())
      {
         if (entry.getValue().intValue() > 1)
         {
            System.out.println("I=" + entry.getKey() + " was received in duplicate, " + entry.getValue() + " times");
            failed = true;
         }
      }

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         if (mapCounter.get(Integer.valueOf(i)) == null)
         {
            System.out.println("Message with i=" + i + " was not received");
            failed = true;
         }
      }

      Assert.assertFalse(failed);

      session.close();
   }


   class MyThread extends Thread
   {
      final String name;
      public MyThread(String name)
      {
         super(name);
         this.name = name;

      }

      public void run()
      {
         try
         {
            ClientSessionFactory factory = locator.createSessionFactory();
            ClientSession session1 = factory.createSession();
            ClientConsumer consumerLocal = session1.createConsumer(MDBQUEUEPREFIXED);
            session1.start();
            while (running.get())
            {
               ClientMessage message = consumerLocal.receive(1000);
               if (message != null)
               {
                  message.acknowledge();
                  session1.commit();

                  Integer value = message.getIntProperty("i");

                  AtomicInteger mapCount = new AtomicInteger(1);

                  mapCount = mapCounter.putIfAbsent(value, mapCount);

                  if (mapCount != null)
                  {
                     System.out.println("Received in duplicate for " + value + " on thread " + name);
//                     System.exit(-1);
                     mapCount.incrementAndGet();
                  }
               }
            }
            session1.close();
            factory.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

   }

   static Integer txTwoPhaseOutCome = null;

   public static void assertTxOutComeIsOfStatusFinishedError(int txOutCome)
   {
      // check only first trigger of byteman rule
      if (txTwoPhaseOutCome == null)
      {
         txTwoPhaseOutCome = Integer.valueOf(txOutCome);
      }
   }

   public class XADummyEndpoint extends DummyMessageEndpoint
   {
      ClientSession session;

      public CountDownLatch latchEnter = new CountDownLatch(1);
      public CountDownLatch latchWait = new CountDownLatch(1);

      public XADummyEndpoint() throws SystemException
      {
         super(null);
         try
         {
            session = locator.createSessionFactory().createSession(true, false, false);
         }
         catch (Throwable e)
         {
            throw new RuntimeException(e);
         }
      }

      @Override
      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {
         super.beforeDelivery(method);
         try
         {
            DummyTMLocator.tm.begin();
            currentTX = DummyTMLocator.tm.getTransaction();
            currentTX.enlistResource(xaResource);
         }
         catch (Throwable e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

      public void onMessage(Message message)
      {
         System.out.println("onMessage enter");
         super.onMessage(message);

         try
         {
            currentTX.enlistResource(session);
            ClientProducer producer = session.createProducer("jms.queue.outQueue");
            ClientMessage message1 = session.createMessage(true);
            message1.putIntProperty("i", message.getIntProperty("i"));
            producer.send(message1);
            currentTX.delistResource(session, XAResource.TMSUCCESS);

         }
         catch (Exception e)
         {
            e.printStackTrace();
            throw new RuntimeException(e);
         }
         try
         {
            latchEnter.countDown();
            latchWait.await();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         System.out.println("onMessage after wait");

//         try
//         {
//            lastMessage = (HornetQMessage) message;
//            currentTX.enlistResource(session);
//            ClientProducer prod = session.createProducer()
//         }
//         catch (Exception e)
//         {
//            e.printStackTrace();
//         }

      }

      @Override
      public void afterDelivery() throws ResourceException
      {
         try
         {
            DummyTMLocator.tm.commit();
//            currentTX.commit();
         }
         catch (Throwable e)
         {
            // its unsure as to whether the EJB/JCA layer will handle this or throw it to us,
            // either way we don't do anything else so its fine just to throw.
            // NB this will only happen with 2 phase commit
            throw new RuntimeException(e);
         }
         super.afterDelivery();
      }
   }

   public static class DummyTMLocator
   {
      public static TransactionManagerImple tm;

      public static void stopTM()
      {
         try
         {
            TransactionReaper.terminate(true);
            TxControl.disable(true);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         tm = null;
      }

      public static void startTM()
      {
         tm = new TransactionManagerImple();
         TxControl.enable();
      }

      public TransactionManager getTM()
      {
         return tm;
      }
   }
}
