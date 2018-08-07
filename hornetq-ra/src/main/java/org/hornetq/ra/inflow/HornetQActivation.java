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
package org.hornetq.ra.inflow;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.server.recovery.XARecoveryConfig;
import org.hornetq.ra.HornetQRABundle;
import org.hornetq.ra.HornetQRALogger;
import org.hornetq.ra.HornetQRaUtils;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.SensitiveDataCodec;
import org.jboss.logging.Logger;

/**
 * The activation.
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQActivation
{
   private static final Logger logger = Logger.getLogger(HornetQActivation.class);
   /**
    * The onMessage method
    */
   public static final Method ONMESSAGE;

   /**
    * The resource adapter
    */
   private final HornetQResourceAdapter ra;

   /**
    * The activation spec
    */
   private final HornetQActivationSpec spec;

   /**
    * The message endpoint factory
    */
   private final MessageEndpointFactory endpointFactory;

   /**
    * Whether delivery is active
    */
   private final AtomicBoolean deliveryActive = new AtomicBoolean(false);

   /**
    * The destination type
    */
   private boolean isTopic = false;

   /**
    * Is the delivery transacted
    */
   private boolean isDeliveryTransacted;

   private HornetQDestination destination;

   /**
    * The name of the temporary subscription name that all the sessions will share
    */
   private SimpleString topicTemporaryQueue;

   private final List<HornetQMessageHandler> handlers = new ArrayList<HornetQMessageHandler>();

   private HornetQConnectionFactory factory;

   private List<String> nodes = Collections.synchronizedList(new ArrayList<String>());

   private Map<String, Long> removedNodes = new ConcurrentHashMap<String, Long>();

   private boolean lastReceived = false;

   // Whether we are in the failure recovery loop
   private final AtomicBoolean inReconnect = new AtomicBoolean(false);
   private XARecoveryConfig resourceRecovery;

   static
   {
      try
      {
         ONMESSAGE = MessageListener.class.getMethod("onMessage", new Class[]{Message.class});
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Constructor
    *
    * @param ra              The resource adapter
    * @param endpointFactory The endpoint factory
    * @param spec            The activation spec
    * @throws ResourceException Thrown if an error occurs
    */
   public HornetQActivation(final HornetQResourceAdapter ra,
                            final MessageEndpointFactory endpointFactory,
                            final HornetQActivationSpec spec) throws ResourceException
   {
      spec.validate();

      if (logger.isTraceEnabled())
      {
         logger.trace("constructor(" + ra + ", " + endpointFactory + ", " + spec + ")");
      }

      if (ra.isUseMaskedPassword())
      {
         String pass = spec.getOwnPassword();
         if (pass != null)
         {
            SensitiveDataCodec<String> codec = ra.getCodecInstance();

            try
            {
               spec.setPassword(codec.decode(pass));
            }
            catch (Exception e)
            {
               throw new ResourceException(e);
            }
         }
      }

      this.ra = ra;
      this.endpointFactory = endpointFactory;
      this.spec = spec;
      try
      {
         isDeliveryTransacted = endpointFactory.isDeliveryTransacted(HornetQActivation.ONMESSAGE);
      }
      catch (Exception e)
      {
         throw new ResourceException(e);
      }
   }

   /**
    * Get the activation spec
    *
    * @return The value
    */
   public HornetQActivationSpec getActivationSpec()
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("getActivationSpec()");
      }

      return spec;
   }

   /**
    * Get the message endpoint factory
    *
    * @return The value
    */
   public MessageEndpointFactory getMessageEndpointFactory()
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("getMessageEndpointFactory()");
      }

      return endpointFactory;
   }

   /**
    * Get whether delivery is transacted
    *
    * @return The value
    */
   public boolean isDeliveryTransacted()
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("isDeliveryTransacted()");
      }

      return isDeliveryTransacted;
   }

   /**
    * Get the work manager
    *
    * @return The value
    */
   public WorkManager getWorkManager()
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("getWorkManager()");
      }

      return ra.getWorkManager();
   }

   /**
    * Is the destination a topic
    *
    * @return The value
    */
   public boolean isTopic()
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("isTopic()");
      }

      return isTopic;
   }

   /**
    * Start the activation
    *
    * @throws ResourceException Thrown if an error occurs
    */
   public void start() throws ResourceException
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("start()");
      }
      deliveryActive.set(true);
      scheduleWork(new SetupActivation());
   }

   /**
    * @return the topicTemporaryQueue
    */
   public SimpleString getTopicTemporaryQueue()
   {
      return topicTemporaryQueue;
   }

   /**
    * @param topicTemporaryQueue the topicTemporaryQueue to set
    */
   public void setTopicTemporaryQueue(SimpleString topicTemporaryQueue)
   {
      this.topicTemporaryQueue = topicTemporaryQueue;
   }

   /**
    * @return the list of XAResources for this activation endpoint
    */
   public List<XAResource> getXAResources()
   {
      List<XAResource> xaresources = new ArrayList<XAResource>();
      for (HornetQMessageHandler handler : handlers)
      {
         XAResource xares = handler.getXAResource();
         if (xares != null)
         {
            xaresources.add(xares);
         }
      }
      return xaresources;
   }

   /**
    * Stop the activation
    */
   public void stop()
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("stop()");
      }

      deliveryActive.set(false);
      teardown(true);
   }

   /**
    * Setup the activation
    *
    * @throws Exception Thrown if an error occurs
    */
   protected synchronized void setup() throws Exception
   {
      logger.debug("Setting up " + spec);

      setupCF();

      setupDestination();

      Exception firstException = null;

      for (int i = 0; i < spec.getMaxSession(); i++)
      {
         ClientSessionFactory cf = null;
         ClientSession session = null;

         try
         {
            cf = factory.getServerLocator().createSessionFactory();
            if (HornetQResourceAdapter.DEBUG_RECONNECTS)
            {
               cf.setDebugReconnects("HornetQActivation::" + this.endpointFactory);
            }
            session = setupSession(cf);
            HornetQMessageHandler handler = new HornetQMessageHandler(factory, this, ra.getTM(), (ClientSessionInternal) session, cf, i);
            handler.setup();
            handlers.add(handler);
         }
         catch (Exception e)
         {
            if (cf != null)
            {
               cf.close();
            }
            if (session != null)
            {
               session.close();
            }
            if (firstException == null)
            {
               firstException = e;
            }
         }
      }
      //if we have any exceptions close all the handlers and throw the first exception.
      //we don't want partially configured activations, i.e. only 8 out of 15 sessions started so best to stop and log the error.
      if (firstException != null)
      {
         for (HornetQMessageHandler handler : handlers)
         {
            handler.teardown();
         }
         throw firstException;
      }

      //now start them all together.
      for (HornetQMessageHandler handler : handlers)
      {
         handler.start();
      }

      resourceRecovery = ra.getRecoveryManager().register(factory, spec.getUser(), spec.getPassword());
      if (spec.isRebalanceConnections())
      {
         factory.getServerLocator().addClusterTopologyListener(new RebalancingListener());
      }

      logger.debug("Setup complete " + this);
   }

   /**
    * Teardown the activation
    */
   protected synchronized void teardown(boolean useInterrupt)
   {
      logger.debug("Tearing down " + spec);

      if (resourceRecovery != null)
      {
         ra.getRecoveryManager().unRegister(resourceRecovery);
      }

      final HornetQMessageHandler[] handlersCopy = new HornetQMessageHandler[handlers.size()];

      // We need to do from last to first as any temporary queue will have been created on the first handler
      // So we invert the handlers here
      for (int i = 0; i < handlers.size(); i++)
      {
         // The index here is the complimentary so it's inverting the array
         handlersCopy[i] = handlers.get(handlers.size() - i - 1);
      }

      handlers.clear();

      FutureLatch future = new FutureLatch(handlersCopy.length);
      for (HornetQMessageHandler handler : handlersCopy)
      {
         handler.interruptConsumer(future);
      }

      //wait for all the consumers to complete any onmessage calls
      boolean stuckThreads = !future.await(factory.getCallTimeout());
      //if any are stuck then we need to interrupt them
      if (stuckThreads)
      {
         if (useInterrupt && spec.getInterruptOnTearDown())
         {
            for (HornetQMessageHandler handler : handlersCopy)
            {
               Thread interruptThread = handler.getCurrentThread();
               if (interruptThread != null)
               {
                  try
                  {
                     interruptThread.interrupt();
                  }
                  catch (Throwable e)
                  {
                     //ok
                  }
               }
            }
         }
      }

      Runnable runTearDown = new Runnable()
      {
         @Override
         public void run()
         {
            for (HornetQMessageHandler handler : handlersCopy)
            {
               handler.teardown();
            }
         }
      };

      Thread threadTearDown = startThread("TearDown/HornetQActivation", runTearDown);

      try
      {
         threadTearDown.join(factory.getCallTimeout());
      }
      catch (InterruptedException e)
      {
         // nothing to be done on this context.. we will just keep going as we need to send an interrupt to threadTearDown and give up
      }

      if (factory != null)
      {
         try
         {
            // closing the factory will help making sure pending threads are closed
            factory.close();
         }
         catch (Throwable e)
         {
            HornetQRALogger.LOGGER.warn(e);
         }

         factory = null;
      }


      if (threadTearDown.isAlive())
      {
         threadTearDown.interrupt();

         try
         {
            threadTearDown.join(5000);
         }
         catch (InterruptedException e)
         {
            // nothing to be done here.. we are going down anyways
         }

         if (threadTearDown.isAlive())
         {
            HornetQRALogger.LOGGER.warn("Thread " + threadTearDown + " couldn't be finished");
         }
      }


      nodes.clear();
      lastReceived = false;

      logger.debug("Tearing down complete " + this);
   }

   protected void setupCF() throws Exception
   {
      factory = ra.newConnectionFactory(spec);
   }

   /**
    * Setup a session
    *
    * @param cf
    * @return The connection
    * @throws Exception Thrown if an error occurs
    */
   protected ClientSession setupSession(ClientSessionFactory cf) throws Exception
   {
      ClientSession result = null;

      try
      {
         result = ra.createSession(cf,
                                   spec.getAcknowledgeModeInt(),
                                   spec.getUser(),
                                   spec.getPassword(),
                                   ra.getPreAcknowledge(),
                                   ra.getDupsOKBatchSize(),
                                   ra.getTransactionBatchSize(),
                                   isDeliveryTransacted,
                                   spec.isUseLocalTx(),
                                   spec.getTransactionTimeout());

         result.addMetaData("resource-adapter", "inbound");
         result.addMetaData("jms-session", "");
         String clientID = ra.getClientID() == null ? spec.getClientID() : ra.getClientID();
         if (clientID != null)
         {
            result.addMetaData("jms-client-id", clientID);
         }

         logger.debug("Using queue connection " + result);

         return result;
      }
      catch (Throwable t)
      {
         try
         {
            if (result != null)
            {
               result.close();
            }
         }
         catch (Exception e)
         {
            logger.trace("Ignored error closing connection", e);
         }
         if (t instanceof Exception)
         {
            throw (Exception) t;
         }
         throw new RuntimeException("Error configuring connection", t);
      }
   }

   public SimpleString getAddress()
   {
      return destination.getSimpleAddress();
   }

   protected void setupDestination() throws Exception
   {

      String destinationName = spec.getDestination();

      if (spec.isUseJNDI())
      {
         Context ctx;
         if (spec.getParsedJndiParams() == null)
         {
            ctx = new InitialContext();
         }
         else
         {
            ctx = new InitialContext(spec.getParsedJndiParams());
         }
         logger.debug("Using context " + ctx.getEnvironment() + " for " + spec);
         if (logger.isTraceEnabled())
         {
            logger.trace("setupDestination(" + ctx + ")");
         }

         String destinationTypeString = spec.getDestinationType();
         if (destinationTypeString != null && !destinationTypeString.trim().equals(""))
         {
            logger.debug("Destination type defined as " + destinationTypeString);

            Class<?> destinationType;
            if (Topic.class.getName().equals(destinationTypeString))
            {
               destinationType = Topic.class;
               isTopic = true;
            }
            else
            {
               destinationType = Queue.class;
            }

            logger.debug("Retrieving " + destinationType.getName() + " \"" + destinationName + "\" from JNDI");

            try
            {
               destination = (HornetQDestination) HornetQRaUtils.lookup(ctx, destinationName, destinationType);
            }
            catch (Exception e)
            {
               if (destinationName == null)
               {
                  throw HornetQRABundle.BUNDLE.noDestinationName();
               }

               String calculatedDestinationName = destinationName.substring(destinationName.lastIndexOf('/') + 1);

               logger.debug("Unable to retrieve " + destinationName +
                               " from JNDI. Creating a new " + destinationType.getName() +
                               " named " + calculatedDestinationName + " to be used by the MDB.");

               // If there is no binding on naming, we will just create a new instance
               if (isTopic)
               {
                  destination = (HornetQDestination) HornetQJMSClient.createTopic(calculatedDestinationName);
               }
               else
               {
                  destination = (HornetQDestination) HornetQJMSClient.createQueue(calculatedDestinationName);
               }
            }
         }
         else
         {
            logger.debug("Destination type not defined in MDB activation configuration.");
            logger.debug("Retrieving " + Destination.class.getName() + " \"" + destinationName + "\" from JNDI");

            destination = (HornetQDestination) HornetQRaUtils.lookup(ctx, destinationName, Destination.class);
            if (destination instanceof Topic)
            {
               isTopic = true;
            }
         }
      }
      else
      {
         HornetQRALogger.LOGGER.instantiatingDestination(spec.getDestinationType(), spec.getDestination());

         if (Topic.class.getName().equals(spec.getDestinationType()))
         {
            destination = (HornetQDestination) HornetQJMSClient.createTopic(spec.getDestination());
            isTopic = true;
         }
         else
         {
            destination = (HornetQDestination) HornetQJMSClient.createQueue(spec.getDestination());
         }
      }
   }

   /**
    * Get a string representation
    *
    * @return The value
    */
   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();
      buffer.append(HornetQActivation.class.getName()).append('(');
      buffer.append("spec=").append(spec.getClass().getName());
      buffer.append(" mepf=").append(endpointFactory.getClass().getName());
      buffer.append(" active=").append(deliveryActive.get());
      if (spec.getDestination() != null)
      {
         buffer.append(" destination=").append(spec.getDestination());
      }
      buffer.append(" transacted=").append(isDeliveryTransacted);
      buffer.append(')');
      return buffer.toString();
   }

   public void startReconnectThread(final String cause)
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("Starting reconnect Thread " + cause + " on MDB activation " + this);
      }
      try
      {
         scheduleWork(new ReconnectWork(cause));
      }
      catch (Exception e)
      {
         logger.warn("Could not reconnect because worker is down", e);
      }
   }



   private static Thread startThread(String name, Runnable run)
   {
      ClassLoader tccl;

      try
      {
         tccl = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
         {
            public ClassLoader run()
            {
               return HornetQActivation.class.getClassLoader();
            }
         });
      }
      catch (Throwable e)
      {
         logger.warn(e.getMessage(), e);
         tccl = null;
      }

      HornetQThreadFactory factory = new HornetQThreadFactory(name, true, tccl);
      Thread t = factory.newThread(run);
      t.start();
      return t;
   }


   private void scheduleWork(Work run) throws WorkException
   {
      ra.getWorkManager().scheduleWork(run);
   }

   /**
    * Drops all existing connection-related resources and reconnects
    *
    * @param failure if reconnecting in the event of a failure
    */
   public void reconnect(Throwable failure, boolean useInterrupt)
   {
      if (logger.isTraceEnabled())
      {
         logger.trace("reconnecting activation " + this);
      }
      if (failure != null)
      {
         if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.QUEUE_DOES_NOT_EXIST)
         {
            HornetQRALogger.LOGGER.awaitingTopicQueueCreation(getActivationSpec().getDestination());
         }
         else if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.NOT_CONNECTED)
         {
            HornetQRALogger.LOGGER.awaitingJMSServerCreation();
         }
         else
         {
            HornetQRALogger.LOGGER.failureInActivation(failure, spec);
         }
      }
      int reconnectCount = 0;
      int setupAttempts = spec.getSetupAttempts();
      long setupInterval = spec.getSetupInterval();

      // Only enter the reconnect loop once
      if (inReconnect.getAndSet(true))
         return;
      try
      {
         Throwable lastException = failure;
         while (deliveryActive.get() && (setupAttempts == -1 || reconnectCount < setupAttempts))
         {
            teardown(useInterrupt);

            try
            {
               Thread.sleep(setupInterval);
            }
            catch (InterruptedException e)
            {
               logger.debug("Interrupted trying to reconnect " + spec, e);
               break;
            }

            if (reconnectCount < 1)
            {
               HornetQRALogger.LOGGER.attemptingReconnect(spec);
            }
            try
            {
               setup();
               HornetQRALogger.LOGGER.reconnected();
               break;
            }
            catch (Throwable t)
            {
               if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.QUEUE_DOES_NOT_EXIST)
               {
                  if (lastException == null || !(t instanceof HornetQNonExistentQueueException))
                  {
                     lastException = t;
                     HornetQRALogger.LOGGER.awaitingTopicQueueCreation(getActivationSpec().getDestination());
                  }
               }
               else if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.NOT_CONNECTED)
               {
                  if (lastException == null || !(t instanceof HornetQNotConnectedException))
                  {
                     lastException = t;
                     HornetQRALogger.LOGGER.awaitingJMSServerCreation();
                  }
               }
               else
               {
                  HornetQRALogger.LOGGER.errorReconnecting(t, spec);
               }
            }
            ++reconnectCount;
         }
      }
      finally
      {
         // Leaving failure recovery loop
         inReconnect.set(false);
      }
   }

   public HornetQConnectionFactory getConnectionFactory()
   {
      return this.factory;
   }

   /**
    * Handles the setup
    */
   private class SetupActivation implements Work
   {
      public void run()
      {
         try
         {
            setup();
         }
         catch (Throwable t)
         {
            reconnect(t, false);
         }
      }

      public void release()
      {
      }
   }


   /**
    * Handles reconnecting
    */
   private class ReconnectWork implements Work
   {
      final String cause;

      ReconnectWork(String cause)
      {
         this.cause = cause;
      }
      @Override
      public void release()
      {

      }

      @Override
      public void run()
      {
         logger.tracef("Starting reconnect for %s", cause);
         reconnect(null, false);
      }

   }


   private class RebalancingListener implements ClusterTopologyListener
   {
      @Override
      public void nodeUP(TopologyMember member, boolean last)
      {
         boolean newNode = false;

         String id = member.getNodeId();
         if (!nodes.contains(id))
         {
            if (removedNodes.get(id) == null || (removedNodes.get(id) != null && removedNodes.get(id) < member.getUniqueEventID()))
            {
               nodes.add(id);
               newNode = true;
            }
         }

         if (lastReceived && newNode)
         {
            HornetQRALogger.LOGGER.rebalancingConnections("nodeUp " + member.toString());
            startReconnectThread("NodeUP Connection Rebalancer");
         }
         else if (last)
         {
            lastReceived = true;
         }
      }

      @Override
      public void nodeDown(long eventUID, String nodeID)
      {
         if (nodes.remove(nodeID))
         {
            removedNodes.put(nodeID, eventUID);
            HornetQRALogger.LOGGER.rebalancingConnections("nodeDown " + nodeID);
            startReconnectThread("NodeDOWN Connection Rebalancer");
         }
      }
   }
}
