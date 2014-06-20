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

package org.hornetq.amqp.test.minimalclient;

import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;

/**
 * @author Clebert Suconic
 */

public class AMQPClientSPI implements ProtonConnectionSPI
{

   final Channel channel;

   public AMQPClientSPI(Channel channel)
   {
      this.channel = channel;
   }

   @Override
   public Executor newSingleThreadExecutor()
   {
      return null;
   }

   @Override
   public String[] getSASLMechanisms()
   {
      return new String[0];
   }

   @Override
   public void close()
   {

   }

   @Override
   public void output(ByteBuf bytes)
   {
      channel.writeAndFlush(bytes);
   }

   @Override
   public ProtonSessionSPI createSessionSPI()
   {
      return null;
   }
}
