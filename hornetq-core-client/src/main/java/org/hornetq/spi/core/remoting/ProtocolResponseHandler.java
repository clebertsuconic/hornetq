package org.hornetq.spi.core.remoting;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * @author Clebert Suconic
 */

public interface ProtocolResponseHandler
{
   // This is sent when the server is telling the client the node is being disconnected
   public void nodeDisconnected(RemotingConnection conn,  String nodeID);

   public void notifyNodeUp(long uniqueEventID,
                               final String nodeID,
                               final String nodeName,
                               final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                               final boolean isLast);

   // This is sent when any node on the cluster topology is going down
   void notifyNodeDown(final long eventTime, final String nodeID);
}
