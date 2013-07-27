package org.hornetq.spi.core.remoting;

import java.util.Map;

import org.hornetq.core.protocol.core.impl.HornetQClientProtocolManagerFactory;

/**
 * Abstract connector
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public abstract class AbstractConnector implements Connector
{
   protected final Map<String, Object> configuration;

   private static final ClientProtocolManagerFactory protocolWrapperFactory = new HornetQClientProtocolManagerFactory();

   protected AbstractConnector(Map<String, Object> configuration)
   {
      this.configuration = configuration;
   }

   public ClientProtocolManagerFactory getProtocolManagerFactory()
   {
      return protocolWrapperFactory;
   }

}
