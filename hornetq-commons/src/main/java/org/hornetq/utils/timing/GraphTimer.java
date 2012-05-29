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

import org.hornetq.utils.ConcurrentHashSet;

import java.io.PrintStream;
import java.util.Set;
import java.util.Stack;

/**
 * This is an interface where you can use to establish timing measurement in an hierarchical form.
 * you can call open, close, and report to generate a view like this:
 *
 * Look at org.hornetq.utils.timing.GraphTimingTest for some examples on how to use this.
 *
 * @author Clebert
 */
public class GraphTimer
{
   public static ThreadLocal<GraphControl> threadLocal = new ThreadLocal<GraphControl>();

   public static Set<GraphControl> controls = new ConcurrentHashSet<GraphControl>();

   public static volatile boolean active = true;

   private static GraphControl globalControl;

   private synchronized static GraphControl getGlobalControl()
   {
      if (globalControl == null)
      {
         globalControl = new GraphControl("GlobalContext");
         controls.add(globalControl);
      }

      return globalControl;
   }

   /**
    * Return all roots associated with the timing
    * @return
    */
   public static GraphItem[] getElements()
   {
      GraphItem[] graphElements = new GraphItem[controls.size()];
      int count = 0;
      for (GraphControl control : controls)
      {
         graphElements[count++] = control.getRoot();
      }

      return graphElements;
   }

   /**
    * it will print a report of the timed elements
    * @param out
    */
   public synchronized static void report(PrintStream out)
   {
      GraphItem[] graphElements = getElements();
      java.util.Arrays.sort(graphElements, new GraphSorter());
      for (GraphItem el : graphElements)
      {
         el.report(out, 0);
      }
   }

   public static boolean isActive()
   {
      return active;
   }

   public static void setActive(boolean active)
   {
      GraphTimer.active = active;
   }


   /**
    * Starts a measurement
    * @param name
    */
   public static void open(String name)
   {
      if (active)
      {
         GraphControl control = getControl();
         control.open(name);
      }
   }

   /**
    * Finishes a measurement
    * @param name
    */
   public static void close(String name)
   {
      if (active)
      {
         GraphControl control = getControl();
         control.close(name);
      }
   }

   public static void openGlobal(String name)
   {
      if (active)
      {
         getGlobalControl().open(name);
      }
   }

   public static void closeGlobal(String name)
   {
      if (active)
      {
         getGlobalControl().close(name);
      }
   }

   public synchronized static void clear()
   {
      for (GraphControl control : controls)
      {
         control.clear();
      }

      controls.clear();
      globalControl = null;
   }


   /**
    * Returns the control associated with the thread
    * @return
    */
   private static GraphControl getControl()
   {
      GraphControl control = threadLocal.get();
      if (control == null || control.isCleared())
      {
         control = new GraphControl();
         controls.add(control);
         threadLocal.set(control);
      }
      return control;
   }

   private static class GraphControl
   {
      private final GraphItem root;
      private Stack<GraphItem> stack = new Stack<GraphItem>();
      private boolean cleared = false;

      public GraphItem getCurrent()
      {
         if (stack.isEmpty())
         {
            return null;
         }
         else
         {
            return stack.peek();
         }
      }

      public GraphItem getRoot()
      {
         return root;
      }

      public GraphControl()
      {
         this(Thread.currentThread().toString());
      }

      public GraphControl(final String name)
      {
         root = new GraphItem(name);
      }

      public synchronized void clear()
      {
         root.clear();
         stack.clear();
         cleared = true;
      }

      public boolean isCleared()
      {
         return cleared;
      }

      public synchronized void open(final String name)
      {
         GraphItem current = getCurrent();
         GraphItem child;
         if (current == null)
         {
            child = root.getChild(name);
         }
         else
         {
            child = current.getChild(name);
         }
         child.open();
         stack.push(child);
      }

      public synchronized void close(final String name)
      {
         for (;;)
         {
            if (stack.isEmpty())
            {
               System.out.println("Can't find element " + name + " on the stack");
               break;
            }
            GraphItem item = stack.pop();

            item.close();

            if (item.getName().equals(name))
            {
               break;
            }
         }
      }

   }

}
