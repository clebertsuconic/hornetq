package org.hornetq.spi.core.remoting;

import java.util.List;
import java.util.concurrent.locks.Lock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * @author Clebert Suconic
 */

public interface ClientProtocolManager
{

   /// Life Cycle Methods:

   RemotingConnection connect(Connection transportConnection, long callTimeout, long callFailoverTimeout,  List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors, ProtocolResponseHandler protocolResponseHandler);

   RemotingConnection getCurrentConnection();

   void setResponseHandler(ProtocolResponseHandler handler);

   Lock lockSessionCreation();

   boolean waitOnLatch(long milliseconds) throws InterruptedException;

   /**
    * This is to be called when a connection failed and we want to interrupt any communication.
    * This used to be called exitLoop at some point o the code.. with a method named causeExit from ClientSessionFactoryImpl
    */
   void stop();

   boolean isAlive();

   /// Sending methods


   void sendSubscribeTopology(boolean isServer);

   void shakeHands();

   void ping(long connectionTTL);

   void sendNodeAnnounce(long currentEventID, String nodeID, String nodeName, boolean backup, TransportConfiguration config, TransportConfiguration backupConfig);

   SessionContext createSessionChannel(final String name,
                                            final String username,
                                                   final String password,
                                                   final boolean xa,
                                                   final boolean autoCommitSends,
                                                   final boolean autoCommitAcks,
                                                   final boolean preAcknowledge,
                                                   int minLargeMessageSize,
                                                   int confirmationWindowSize) throws HornetQException;

   void cleanupBeforeFailover();
}
