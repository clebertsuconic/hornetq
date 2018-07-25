/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.hornetq.tests.integration.cluster.distribution;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.NetUtil;
import org.hornetq.tests.util.NetUtilResource;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * A SymmetricClusterWithBackupTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Created 13 Mar 2009 11:00:31
 */
public class NetworkFailureOnClusterWithBackupTest extends ServiceTestBase
{

   Process s1Live;
   Process s1Backup;
   Process s2Live;
   Process s2Backup;

   @Rule
   public NetUtilResource netUtilResource = new NetUtilResource();

   @Before
   public void startNetwork() throws Exception
   {
      NetUtil.assumeSudo();
      NetUtil.netUp(IP_XX);
      NetUtil.netUp(IP_S1);
      NetUtil.netUp(IP_S2);
      NetUtil.netUp(IP_B1);
      NetUtil.netUp(IP_B2);
   }

   // 192.0.2.0 is reserved for documentation, so I'm pretty sure this won't exist on any system. (It shouldn't at least)
   private static final String IP_XX = "192.0.2.1"; // this is placeholder for IPs .. the ifdown would break without this
   private static final String IP_S1 = "192.0.2.2";
   private static final String IP_B1 = "192.0.2.3";
   private static final String IP_S2 = "192.0.2.4";
   private static final String IP_B2 = "192.0.2.5";

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   public static final String GROUP_ADDRESS = "226.76.157.45";
   public static final int GROUP_PORT = 9876;

   HashSet<Process> processes = new HashSet<Process>();

   public Process startProcess(String logname, String... arg) throws Exception
   {
      Process p = SpawnedVMSupport.spawnVM(logname, NetworkFailureOnClusterWithBackupTest.class.getName(), "-Xms512m", "-Xmx512m", new String[0], true, true, arg);
      processes.add(p);
      return p;
   }

   @After
   public void killProcesses() throws Exception
   {
      for (Process p : processes)
      {
         p.destroy();
      }
   }

   protected static final String CLUSTER_PASSWORD = "UnitTestsClusterPassword";

   public static void main(String... arg)
   {
      if (arg.length != 4)
      {
         System.err.println("Usage java -cp <classpath> " + NetworkFailureOnClusterWithBackupTest.class.getName() + " BACKUP|LIVE IP PORT folder");
         System.exit(-1);
      }

      try
      {
         String type = arg[0];
         String ip = arg[1];
         int port = Integer.parseInt(arg[2]);
         String folder = arg[3];
         System.out.println("Journal:: " + folder);
         HornetQServer server = createServer(type.equals("BACKUP"), ip, port, folder);
         server.start();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-2);
      }
   }

   private void downAndCrash(String name, String ip, Process p) throws Exception
   {
      System.out.println("Down network for " + name);
      NetUtil.netDown(ip);
      Thread.sleep(5000);
      System.out.println("Crash server " + name);
      p.destroy();
      Thread.sleep(1000);
   }

   @Test
   public void testCrashes() throws Exception
   {

      startS1Live();
      Thread.sleep(1000);
      startS1Backup();
      Thread.sleep(1000);
      startS2Live();
      Thread.sleep(1000);
      startS2Backup();

      downAndCrash("S2", IP_S2, s2Live);
      NetUtil.netUp(IP_S2);
      System.out.println("********************** S2 is starting back!!! Attention!!! XXXXXXXX");
      startS2Live(); // Failback

      Thread.sleep(5000);

      downAndCrash("S1", IP_S1, s1Live);


      NetUtil.netUp(IP_S1);
      System.out.println("********************** S1 is starting back!!! Attention!!! XXXXXXXX");
      System.out.flush();
      startS1Live();

      Thread.sleep(10000);
   }

   private void startS2Backup() throws Exception
   {
      s2Backup = startProcess("backup_2", "BACKUP", IP_B2, "5445", getJournalDir(2, false));
   }

   private void startS2Live() throws Exception
   {
      s2Live = startProcess("live_2", "LIVE", IP_S2, "5445", getJournalDir(2, false));
   }

   private void startS1Backup() throws Exception
   {
      s1Backup = startProcess("backup_1","BACKUP", IP_B1, "5445", getJournalDir(1, false));
   }

   private void startS1Live() throws Exception
   {
      s1Live = startProcess("live_1","LIVE", IP_S1, "5445", getJournalDir(1, false));
   }

   public static HornetQServer createServer(boolean backup, String ip, int port, String folder) throws Exception
   {

      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(JournalType.NIO);

      String journalFolder = folder + "/journal";
      String bindingFolder = folder + "/binding";
      String pagingFolder = folder + "/paging";
      String largeMessageFolder = folder + "/largeMessages";

      configuration.setJournalDirectory(journalFolder);
      configuration.setBindingsDirectory(bindingFolder);
      configuration.setPagingDirectory(pagingFolder);
      configuration.setLargeMessagesDirectory(largeMessageFolder);
      configuration.setAllowAutoFailBack(true);

      configuration.setJournalCompactMinFiles(0);
      configuration.setJournalCompactPercentage(0);
      configuration.setClusterPassword(CLUSTER_PASSWORD);


      configuration.setJournalMaxIO_AIO(1000);
      configuration.setSharedStore(true);
      configuration.setCheckForLiveServer(true);
      configuration.setThreadPoolMaxSize(10);
      configuration.setBackup(backup);

      configuration.getAcceptorConfigurations().clear();


      TransportConfiguration acceptorConfig = createAcceptorConfig(ip, port);
      configuration.getAcceptorConfigurations().add(acceptorConfig);

      TransportConfiguration connectorConfig = createConnectorConfig(ip, port);

      configuration.getConnectorConfigurations().put("netty", connectorConfig);

      createBroadcastConfig(configuration, "bc1", GROUP_ADDRESS, GROUP_PORT, null, -1, "netty");
      createDiscoveryConfiguration(configuration, "dc1", GROUP_ADDRESS, GROUP_PORT, null, -1);

      ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration("my-cluster",
                                                                                                         "jms",
                                                                                                         "netty",
                                                                                                         HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterFailureCheckPeriod(),
                                                                                                         1000, // HornetQDefaultConfiguration.getDefaultClusterConnectionTtl(),
                                                                                                         100, //HornetQDefaultConfiguration.getDefaultClusterRetryInterval(),
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier(),
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterMaxRetryInterval(),
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterReconnectAttempts(),
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterConnectionTtl(),
                                                                                                         HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                                                                         HornetQDefaultConfiguration.isDefaultClusterDuplicateDetection(),
                                                                                                         HornetQDefaultConfiguration.isDefaultClusterForwardWhenNoConsumers(),
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterMaxHops(),
                                                                                                         FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                                                                         "dc1",
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterNotificationInterval(),
                                                                                                         HornetQDefaultConfiguration.getDefaultClusterNotificationAttempts());

      ArrayList<ClusterConnectionConfiguration> clusterConnectionConfigurations = new ArrayList<ClusterConnectionConfiguration>();
      clusterConnectionConfigurations.add(clusterConnectionConfiguration);
      configuration.setClusterConfigurations(clusterConnectionConfigurations);

      HornetQServer server = new HornetQServerImpl(configuration, ManagementFactory.getPlatformMBeanServer());

      return server;
   }

   public static BroadcastGroupConfiguration createBroadcastConfig(ConfigurationImpl configuration, String name, String groupAddress, int groupPort, String localBindAddress, int localBindPort, String... connector)
   {

      ArrayList<String> connectorName = new ArrayList<String>();
      for (String a : connector)
      {
         connectorName.add(a);
      }

      final long broadcastPeriod = 5000;

      BroadcastGroupConfiguration bcconfig = new BroadcastGroupConfiguration(name,
                                                                             broadcastPeriod,
                                                                             connectorName,
                                                                             new UDPBroadcastGroupConfiguration(groupAddress, groupPort, localBindAddress, localBindPort));

      if (configuration != null)
      {
         ArrayList<BroadcastGroupConfiguration> list = new ArrayList<BroadcastGroupConfiguration>();
         list.add(bcconfig);
         configuration.setBroadcastGroupConfigurations(list);
      }

      return bcconfig;
   }
   public static DiscoveryGroupConfiguration createDiscoveryConfiguration(ConfigurationImpl configuration, String name, String groupAddress, int groupPort, String localBindAddress, int localBindPort)
   {

      UDPBroadcastGroupConfiguration udpConfig = new UDPBroadcastGroupConfiguration(groupAddress, groupPort, localBindAddress, localBindPort);
      DiscoveryGroupConfiguration discoveryGroupConfiguration = new DiscoveryGroupConfiguration(name, 10000, 10000, udpConfig);

      if (configuration != null)
      {
         Map<String, DiscoveryGroupConfiguration> list = new HashMap<String, DiscoveryGroupConfiguration>();
         list.put(name, discoveryGroupConfiguration);
         configuration.setDiscoveryGroupConfigurations(list);
      }

      return discoveryGroupConfiguration;
   }

   public static TransportConfiguration createAcceptorConfig(String ip, int port)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(TransportConstants.PORT_PROP_NAME,
                 port);

      params.put(TransportConstants.HOST_PROP_NAME, ip);

      return new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(), params);
   }


   public static TransportConfiguration createConnectorConfig(String ip, int port)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(TransportConstants.PORT_PROP_NAME,
                 port);

      params.put(TransportConstants.HOST_PROP_NAME, ip);

      return new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), params);
   }

   /*
   @Override
   @Test
   public void testStopAllStartAll() throws Throwable
   {
      try
      {
         setupCluster();

         startServers();

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());
         setupSessionFactory(3, isNetty());
         setupSessionFactory(4, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);
         createQueue(3, "queues.testaddress", "queue0", null, false);
         createQueue(4, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);
         addConsumer(3, 3, "queue0", null);
         addConsumer(4, 4, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);
         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 4, 4, false);
         waitForBindings(1, "queues.testaddress", 4, 4, false);
         waitForBindings(2, "queues.testaddress", 4, 4, false);
         waitForBindings(3, "queues.testaddress", 4, 4, false);
         waitForBindings(4, "queues.testaddress", 4, 4, false);

         System.out.println("waited for all bindings");

         send(0, "queues.testaddress", 10, false, null);

         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2, 3, 4);

         verifyNotReceive(0, 1, 2, 3, 4);

         closeAllConsumers();

         closeAllSessionFactories();

         closeAllServerLocatorsFactories();

         stopServers();

         startServers();

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());
         setupSessionFactory(3, isNetty());
         setupSessionFactory(4, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, false);
         createQueue(1, "queues.testaddress", "queue0", null, false);
         createQueue(2, "queues.testaddress", "queue0", null, false);
         createQueue(3, "queues.testaddress", "queue0", null, false);
         createQueue(4, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);
         addConsumer(3, 3, "queue0", null);
         addConsumer(4, 4, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);
         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 4, 4, false);
         waitForBindings(1, "queues.testaddress", 4, 4, false);
         waitForBindings(2, "queues.testaddress", 4, 4, false);
         waitForBindings(3, "queues.testaddress", 4, 4, false);
         waitForBindings(4, "queues.testaddress", 4, 4, false);

         send(0, "queues.testaddress", 10, false, null);

         verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2, 3, 4);

         verifyNotReceive(0, 1, 2, 3, 4);
      }
      catch (Throwable e)
      {
         System.out.println(UnitTestCase.threadDump("SymmetricClusterWithBackupTest::testStopAllStartAll"));
         throw e;
      }
   }

   @Override
   @Test
   public void testMixtureLoadBalancedAndNonLoadBalancedQueuesAddQueuesAndConsumersBeforeAllServersAreStarted() throws Exception
   {
      setupCluster();

      startServers(5, 0);
      setupSessionFactory(0, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(0, "queues.testaddress", "queue5", null, false);
      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(0, "queues.testaddress", "queue15", null, false);
      createQueue(0, "queues.testaddress", "queue17", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(5, 0, "queue5", null);

      startServers(6, 1);
      setupSessionFactory(1, isNetty());

      createQueue(1, "queues.testaddress", "queue1", null, false);
      createQueue(1, "queues.testaddress", "queue6", null, false);
      createQueue(1, "queues.testaddress", "queue11", null, false);
      createQueue(1, "queues.testaddress", "queue15", null, false);
      createQueue(1, "queues.testaddress", "queue17", null, false);

      addConsumer(1, 1, "queue1", null);
      addConsumer(6, 1, "queue6", null);
      addConsumer(11, 1, "queue11", null);
      addConsumer(16, 1, "queue15", null);

      startServers(7, 2);
      setupSessionFactory(2, isNetty());

      createQueue(2, "queues.testaddress", "queue2", null, false);
      createQueue(2, "queues.testaddress", "queue7", null, false);
      createQueue(2, "queues.testaddress", "queue12", null, false);
      createQueue(2, "queues.testaddress", "queue15", null, false);
      createQueue(2, "queues.testaddress", "queue16", null, false);

      addConsumer(2, 2, "queue2", null);
      addConsumer(7, 2, "queue7", null);
      addConsumer(12, 2, "queue12", null);
      addConsumer(17, 2, "queue15", null);

      startServers(8, 3);
      setupSessionFactory(3, isNetty());

      createQueue(3, "queues.testaddress", "queue3", null, false);
      createQueue(3, "queues.testaddress", "queue8", null, false);
      createQueue(3, "queues.testaddress", "queue13", null, false);
      createQueue(3, "queues.testaddress", "queue15", null, false);
      createQueue(3, "queues.testaddress", "queue16", null, false);
      createQueue(3, "queues.testaddress", "queue18", null, false);

      addConsumer(3, 3, "queue3", null);
      addConsumer(8, 3, "queue8", null);
      addConsumer(13, 3, "queue13", null);
      addConsumer(18, 3, "queue15", null);

      startServers(9, 4);
      setupSessionFactory(4, isNetty());

      createQueue(4, "queues.testaddress", "queue4", null, false);
      createQueue(4, "queues.testaddress", "queue9", null, false);
      createQueue(4, "queues.testaddress", "queue14", null, false);
      createQueue(4, "queues.testaddress", "queue15", null, false);
      createQueue(4, "queues.testaddress", "queue16", null, false);
      createQueue(4, "queues.testaddress", "queue17", null, false);
      createQueue(4, "queues.testaddress", "queue18", null, false);

      addConsumer(4, 4, "queue4", null);
      addConsumer(9, 4, "queue9", null);
      addConsumer(10, 0, "queue10", null);
      addConsumer(14, 4, "queue14", null);

      addConsumer(15, 0, "queue15", null);
      addConsumer(19, 4, "queue15", null);

      addConsumer(20, 2, "queue16", null);
      addConsumer(21, 3, "queue16", null);
      addConsumer(22, 4, "queue16", null);

      addConsumer(23, 0, "queue17", null);
      addConsumer(24, 1, "queue17", null);
      addConsumer(25, 4, "queue17", null);

      addConsumer(26, 3, "queue18", null);
      addConsumer(27, 4, "queue18", null);

      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(1, "queues.testaddress", 5, 5, true);
      waitForBindings(2, "queues.testaddress", 5, 5, true);
      waitForBindings(3, "queues.testaddress", 6, 6, true);
      waitForBindings(4, "queues.testaddress", 7, 7, true);

      waitForBindings(0, "queues.testaddress", 23, 23, false);
      waitForBindings(1, "queues.testaddress", 23, 23, false);
      waitForBindings(2, "queues.testaddress", 23, 23, false);
      waitForBindings(3, "queues.testaddress", 22, 22, false);
      waitForBindings(4, "queues.testaddress", 21, 21, false);

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

      verifyReceiveRoundRobinInSomeOrder(10, 15, 16, 17, 18, 19);

      verifyReceiveRoundRobinInSomeOrder(10, 20, 21, 22);

      verifyReceiveRoundRobinInSomeOrder(10, 23, 24, 25);

      verifyReceiveRoundRobinInSomeOrder(10, 26, 27);
   }

   @Override
   public void _testStartStopServers() throws Exception
   {
      setupCluster();

      startServers();

      NetworkFailureOnClusterWithBackupTest.log.info("setup session factories: ");

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());
      setupSessionFactory(3, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue1", null, false);
      createQueue(2, "queues.testaddress", "queue2", null, false);
      createQueue(3, "queues.testaddress", "queue3", null, false);
      createQueue(4, "queues.testaddress", "queue4", null, false);

      createQueue(0, "queues.testaddress", "queue5", null, false);
      createQueue(1, "queues.testaddress", "queue6", null, false);
      createQueue(2, "queues.testaddress", "queue7", null, false);
      createQueue(3, "queues.testaddress", "queue8", null, false);
      createQueue(4, "queues.testaddress", "queue9", null, false);

      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(1, "queues.testaddress", "queue11", null, false);
      createQueue(2, "queues.testaddress", "queue12", null, false);
      createQueue(3, "queues.testaddress", "queue13", null, false);
      createQueue(4, "queues.testaddress", "queue14", null, false);

      createQueue(0, "queues.testaddress", "queue15", null, false);
      createQueue(1, "queues.testaddress", "queue15", null, false);
      createQueue(2, "queues.testaddress", "queue15", null, false);
      createQueue(3, "queues.testaddress", "queue15", null, false);
      createQueue(4, "queues.testaddress", "queue15", null, false);

      createQueue(2, "queues.testaddress", "queue16", null, false);
      createQueue(3, "queues.testaddress", "queue16", null, false);
      createQueue(4, "queues.testaddress", "queue16", null, false);

      createQueue(0, "queues.testaddress", "queue17", null, false);
      createQueue(1, "queues.testaddress", "queue17", null, false);
      createQueue(4, "queues.testaddress", "queue17", null, false);

      createQueue(3, "queues.testaddress", "queue18", null, false);
      createQueue(4, "queues.testaddress", "queue18", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue1", null);
      addConsumer(2, 2, "queue2", null);
      addConsumer(3, 3, "queue3", null);
      addConsumer(4, 4, "queue4", null);

      addConsumer(5, 0, "queue5", null);
      addConsumer(6, 1, "queue6", null);
      addConsumer(7, 2, "queue7", null);
      addConsumer(8, 3, "queue8", null);
      addConsumer(9, 4, "queue9", null);

      addConsumer(10, 0, "queue10", null);
      addConsumer(11, 1, "queue11", null);
      addConsumer(12, 2, "queue12", null);
      addConsumer(13, 3, "queue13", null);
      addConsumer(14, 4, "queue14", null);

      addConsumer(15, 0, "queue15", null);
      addConsumer(16, 1, "queue15", null);
      addConsumer(17, 2, "queue15", null);
      addConsumer(18, 3, "queue15", null);
      addConsumer(19, 4, "queue15", null);

      addConsumer(20, 2, "queue16", null);
      addConsumer(21, 3, "queue16", null);
      addConsumer(22, 4, "queue16", null);

      addConsumer(23, 0, "queue17", null);
      addConsumer(24, 1, "queue17", null);
      addConsumer(25, 4, "queue17", null);

      addConsumer(26, 3, "queue18", null);
      addConsumer(27, 4, "queue18", null);

      NetworkFailureOnClusterWithBackupTest.log.info("wait for bindings...");

      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(1, "queues.testaddress", 5, 5, true);
      waitForBindings(2, "queues.testaddress", 5, 5, true);
      waitForBindings(3, "queues.testaddress", 6, 6, true);
      waitForBindings(4, "queues.testaddress", 7, 7, true);

      waitForBindings(0, "queues.testaddress", 23, 23, false);
      waitForBindings(1, "queues.testaddress", 23, 23, false);
      waitForBindings(2, "queues.testaddress", 23, 23, false);
      waitForBindings(3, "queues.testaddress", 22, 22, false);
      waitForBindings(4, "queues.testaddress", 21, 21, false);

      NetworkFailureOnClusterWithBackupTest.log.info("send and receive messages");

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

      verifyReceiveRoundRobinInSomeOrder(10, 15, 16, 17, 18, 19);

      verifyReceiveRoundRobinInSomeOrder(10, 20, 21, 22);

      verifyReceiveRoundRobinInSomeOrder(10, 23, 24, 25);

      verifyReceiveRoundRobinInSomeOrder(10, 26, 27);

      removeConsumer(0);
      removeConsumer(5);
      removeConsumer(10);
      removeConsumer(15);
      removeConsumer(23);
      removeConsumer(3);
      removeConsumer(8);
      removeConsumer(13);
      removeConsumer(18);
      removeConsumer(21);
      removeConsumer(26);

      closeSessionFactory(0);
      closeSessionFactory(3);

      stopServers(5, 8, 0, 3);

      startServers(0, 3, 5, 8);

      Thread.sleep(2000);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(3, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(3, "queues.testaddress", "queue3", null, false);

      createQueue(0, "queues.testaddress", "queue5", null, false);
      createQueue(3, "queues.testaddress", "queue8", null, false);

      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(3, "queues.testaddress", "queue13", null, false);

      createQueue(0, "queues.testaddress", "queue15", null, false);
      createQueue(3, "queues.testaddress", "queue15", null, false);

      createQueue(3, "queues.testaddress", "queue16", null, false);

      createQueue(0, "queues.testaddress", "queue17", null, false);

      createQueue(3, "queues.testaddress", "queue18", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(3, 3, "queue3", null);

      addConsumer(5, 0, "queue5", null);
      addConsumer(8, 3, "queue8", null);

      addConsumer(10, 0, "queue10", null);
      addConsumer(13, 3, "queue13", null);

      addConsumer(15, 0, "queue15", null);
      addConsumer(18, 3, "queue15", null);

      addConsumer(21, 3, "queue16", null);

      addConsumer(23, 0, "queue17", null);

      addConsumer(26, 3, "queue18", null);

      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(1, "queues.testaddress", 5, 5, true);
      waitForBindings(2, "queues.testaddress", 5, 5, true);
      waitForBindings(3, "queues.testaddress", 6, 6, true);
      waitForBindings(4, "queues.testaddress", 7, 7, true);

      waitForBindings(0, "queues.testaddress", 23, 23, false);
      waitForBindings(1, "queues.testaddress", 23, 23, false);
      waitForBindings(2, "queues.testaddress", 23, 23, false);
      waitForBindings(3, "queues.testaddress", 22, 22, false);
      waitForBindings(4, "queues.testaddress", 21, 21, false);

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

      verifyReceiveRoundRobinInSomeOrder(10, 15, 16, 17, 18, 19);

      verifyReceiveRoundRobinInSomeOrder(10, 20, 21, 22);

      verifyReceiveRoundRobinInSomeOrder(10, 23, 24, 25);

      verifyReceiveRoundRobinInSomeOrder(10, 26, 27);
   }

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      // The lives
      setupClusterConnectionWithBackups("cluster0",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        0,
                                        new int[]{1, 2, 3, 4});

      setupClusterConnectionWithBackups("cluster1",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        1,
                                        new int[]{0, 2, 3, 4});

      setupClusterConnectionWithBackups("cluster2",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        2,
                                        new int[]{0, 1, 3, 4});

      setupClusterConnectionWithBackups("cluster3",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        3,
                                        new int[]{0, 1, 2, 4});

      setupClusterConnectionWithBackups("cluster4",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        4,
                                        new int[]{0, 1, 2, 3});

      // The backups

      setupClusterConnectionWithBackups("cluster0",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        5,
                                        new int[]{0, 1, 2, 3, 4});

      setupClusterConnectionWithBackups("cluster1",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        6,
                                        new int[]{0, 1, 2, 3, 4});

      setupClusterConnectionWithBackups("cluster2",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        7,
                                        new int[]{0, 1, 2, 3, 4});

      setupClusterConnectionWithBackups("cluster3",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        8,
                                        new int[]{0, 1, 2, 3, 4});

      setupClusterConnectionWithBackups("cluster4",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        9,
                                        new int[]{0, 1, 2, 3, 4});
   }

   @Override
   protected void setupServers() throws Exception
   {
      // The backups
      setupBackupServer(5, 0, isFileStorage(), true, isNetty());
      setupBackupServer(6, 1, isFileStorage(), true, isNetty());
      setupBackupServer(7, 2, isFileStorage(), true, isNetty());
      setupBackupServer(8, 3, isFileStorage(), true, isNetty());
      setupBackupServer(9, 4, isFileStorage(), true, isNetty());

      // The lives
      setupLiveServer(0, isFileStorage(), true, isNetty());
      setupLiveServer(1, isFileStorage(), true, isNetty());
      setupLiveServer(2, isFileStorage(), true, isNetty());
      setupLiveServer(3, isFileStorage(), true, isNetty());
      setupLiveServer(4, isFileStorage(), true, isNetty());
   }

   @Override
   protected void startServers() throws Exception
   {
      // Need to set backup, since when restarting backup after it has failed over, backup will have been set to false

      getServer(5).getConfiguration().setBackup(true);
      getServer(6).getConfiguration().setBackup(true);
      getServer(7).getConfiguration().setBackup(true);
      getServer(8).getConfiguration().setBackup(true);
      getServer(9).getConfiguration().setBackup(true);

      startServers(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
   }

   @Override
   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(5, 6, 7, 8, 9, 0, 1, 2, 3, 4);
   } */

}
