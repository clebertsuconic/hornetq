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

package org.hornetq.amqp.test.minimalserver;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.hornetq.amqp.dealer.util.ByteUtil;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.DebugInfo;

/**
 * @author Clebert Suconic
 */

public class MinimalConnectionSPI implements ProtonConnectionSPI
{
   Channel channel;


   public MinimalConnectionSPI(Channel channel)
   {
      this.channel = channel;
   }

   ExecutorService executorService = Executors.newSingleThreadExecutor();
   @Override
   public Executor newSingleThreadExecutor()
   {
      return executorService;
   }

   @Override
   public String[] getSASLMechanisms()
   {
      return new String[] {"PLAIN"};
   }

   @Override
   public void close()
   {
      executorService.shutdown();
   }

   @Override
   public void output(ByteBuf bytes)
   {

      if (DebugInfo.debug)
      {
         // some debug
         byte[] frame = new byte[bytes.writerIndex()];
         int readerOriginalPos = bytes.readerIndex();

         bytes.getBytes(0, frame);

         try
         {
            System.err.println("Buffer Outgoing: " + "\n" + ByteUtil.formatGroup(ByteUtil.bytesToHex(frame), 4, 16));
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         bytes.readerIndex(readerOriginalPos);
      }


      // ^^ debug

      channel.writeAndFlush(bytes);
   }

   @Override
   public ProtonSessionSPI createSessionSPI()
   {
      return new MinimalSessionSPI();
   }
}
