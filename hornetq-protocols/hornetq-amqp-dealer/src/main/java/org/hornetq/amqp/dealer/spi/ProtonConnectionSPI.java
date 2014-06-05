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

package org.hornetq.amqp.dealer.spi;

import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;

/**
 * @author Clebert Suconic
 */

public interface ProtonConnectionSPI
{
   /**
    * This should return a single thread executor that would consume threads from your thread pool.
    * On HornetQ we have an OrderedExecutor that will consume of a multi-thread main executor.
    * */
   Executor newSingleThreadExecutor();

   String[] getSASLMechanisms();

   void close();

   void output(ByteBuf bytes);

   ProtonSessionSPI createSessionSPI();
}
