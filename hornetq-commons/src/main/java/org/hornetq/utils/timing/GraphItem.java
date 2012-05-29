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

import com.sun.org.apache.bcel.internal.generic.NEW;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the POJO used to generate the graph
 */
public class GraphItem
{
   private final String name;

   private long calls;

   private long totalTime;

   private long timeStart;

   private Map<String, GraphItem> children;

   public GraphItem(String name)
   {
      this.name = name;
   }

   public String getName()
   {
      return name;
   }

   public void open()
   {
      timeStart = System.currentTimeMillis();
   }

   public long getCalls()
   {
      return calls;
   }

   public long getTotalTime()
   {
      return totalTime;
   }

   public void close()
   {
      if (timeStart == 0)
      {
          System.out.println("graph " + name + " wasn't open, ignoring call on close");
      }
      else
      {
         calls++;
         totalTime += System.currentTimeMillis() - timeStart;
      }
      timeStart = 0;
   }

   public GraphItem[] getChildren()
   {
      if (children == null)
      {
         return new GraphItem[0];
      }
      else
      {
         GraphItem[] childrenArray = new GraphItem[children.size()];
         return children.values().toArray(childrenArray);
      }
   }

   public GraphItem getChild(String name)
   {
      Map<String, GraphItem> map = getChildrenMap();
      GraphItem child = map.get(name);

      if (child == null)
      {
         child = new GraphItem(name);
         map.put(name, child);
      }

      return child;
   }

   public synchronized void report(PrintStream buffer, int level)
   {
      for (int i = 0 ; i < level; i++)
      {
         buffer.print('.');
      }
      buffer.println(this.toString());

      GraphItem[] children = getChildren();

      java.util.Arrays.sort(children, new GraphSorter());

      for (GraphItem child : children)
      {
         child.report(buffer, level+1);
      }
   }


   @Override
   public String toString()
   {
      return name + "{" +
         "calls=" + calls +
         ", totalTime=" + totalTime +
         ", avgTime=" + ((double)totalTime / (double)calls) +
         '}';
   }

   public void clear()
   {
      children.clear();
      children = null;
      calls = 0;
      totalTime = 0;
      timeStart = 0;
   }

   private Map<String, GraphItem> getChildrenMap()
   {
      if (children == null)
      {
         children = new HashMap<String, GraphItem>();
      }

      return children;
   }
}
