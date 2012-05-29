package org.hornetq.utils.timing;

import junit.framework.TestCase;

/**
 * Created with IntelliJ IDEA.
 * User: clebert
 * Date: 6/7/12
 * Time: 12:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class GraphTimingTest extends TestCase
{
   public void testSampleTime() throws Exception
   {
      GraphTimer.open("testSampleTime");
      Thread.sleep(50);
      GraphTimer.open("sub-method");
      Thread.sleep(30);
      GraphTimer.close("sub-method");
      GraphTimer.close("testSampleTime");

      GraphTimer.report(System.out);
      GraphTimer.clear();
   }

   public void testSort() throws Exception
   {
      GraphTimer.open("testA");
      Thread.sleep(30);
      GraphTimer.close("testA");
      GraphTimer.open("testC");
      Thread.sleep(15);
      GraphTimer.close("testC");

      GraphTimer.open("testB");
      Thread.sleep(50);
      GraphTimer.close("testB");

      GraphTimer.report(System.out);
      GraphTimer.clear();
   }

   public void testSampleTime2() throws Exception
   {
      GraphTimer.open("testSampleTime");
      Thread.sleep(50);
      GraphTimer.open("sub-method");
      Thread.sleep(30);
      GraphTimer.close("testSampleTime");

      GraphTimer.report(System.out);
      GraphTimer.clear();
   }
}
