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

package org.hornetq.tools;

import java.util.LinkedList;
import java.util.List;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.JournalRecordIds;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;

/**
 * Writes a human-readable interpretation of the contents of a HornetQ {@link org.hornetq.core.journal.Journal}.
 * <p>
 * To run this class with Maven, use:
 * <p>
 * <pre>
 * cd hornetq-server
 * mvn -q exec:java -Dexec.args="/foo/hornetq/bindings /foo/hornetq/journal" -Dexec.mainClass="org.hornetq.tools.PrintData"
 * </pre>
 *
 * @author clebertsuconic
 */
public class ExportQueues // NO_UCD (unused code)
{

   protected static void exportQueues(String bindingsDirectory)
   {
      try
      {
         printQueues(bindingsDirectory);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }


   public static void printQueues(final String bindingsDir) throws Exception
   {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null);

      JournalImpl journal = new JournalImpl(1024 * 1024, 2, -1, 0, bindingsFF, "hornetq-bindings", "bindings", 1);


      List<RecordInfo> records = new LinkedList<RecordInfo>();
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

      journal.start();

      journal.load(records, preparedTransactions, new TransactionFailureCallback()
      {

         @Override
         public void failedTransaction(long transactionID, List<RecordInfo> records1, List<RecordInfo> recordsToDelete)
         {
         }
      }, false);


      for (RecordInfo info : records)
      {
         if (info.getUserRecordType() == JournalRecordIds.QUEUE_BINDING_RECORD)
         {
            HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(info.data);
            JournalStorageManager.PersistentQueueBindingEncoding bindingEncoding = new JournalStorageManager.PersistentQueueBindingEncoding();

            bindingEncoding.decode(buffer);

            bindingEncoding.setId(info.id);


            // TODO make the formatting here
            System.out.println("##########################");
            System.out.println("The address Name is:" + bindingEncoding.address);
            System.out.println("The queue Name is:" + bindingEncoding.getQueueName());

         }
      }

      journal.stop();

   }
}
