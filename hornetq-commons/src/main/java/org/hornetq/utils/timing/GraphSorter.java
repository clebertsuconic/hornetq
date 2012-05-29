/*
 * Copyright 2010 Red Hat, Inc.
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
package org.hornetq.utils.timing;

import java.util.Comparator;

/**
 * this is just an utility class to sort results before reports
 * @author Clebert
 */
class GraphSorter implements Comparator<GraphItem>
{
   public int compare(GraphItem t1, GraphItem t2)
   {
      long value = t2.getTotalTime() - t1.getTotalTime();


      if (value == 0)
      {
         return t1.getName().compareTo(t2.getName());
      } else if (value > 1)
      {
         return 1;
      } else
      {
         return -1;
      }

   }

   public boolean equals(Object o)
   {
      return this.equals(o);
   }
}