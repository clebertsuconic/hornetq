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

package org.hornetq.amqp.dealer;

/**
 * @author Clebert Suconic
 */

public class ByteUtil
{


   public static String formatGroup(String str, int groupSize, int lineBreak)
   {
      StringBuffer buffer = new StringBuffer();

      int line = 1;
      buffer.append("/*  1 */ \"");
      for (int i = 0; i < str.length(); i += groupSize)
      {
         buffer.append(str.substring(i, i + Math.min(str.length() - i, groupSize)));

         if ((i + groupSize) % lineBreak == 0)
         {
            buffer.append("\" +\n/* ");
            line++;
            if (line < 10)
            {
               buffer.append(" ");
            }
            buffer.append(Integer.toString(line) + " */ \"");
         }
         else if ((i + groupSize) % groupSize == 0 && str.length() - i > groupSize)
         {
            buffer.append("\" + \"");
         }
      }

      buffer.append("\";");

      return buffer.toString();

   }

   protected static final char[] hexArray = "0123456789ABCDEF".toCharArray();

   public static String bytesToHex(byte[] bytes)
   {
      char[] hexChars = new char[bytes.length * 2];
      for (int j = 0; j < bytes.length; j++)
      {
         int v = bytes[j] & 0xFF;
         hexChars[j * 2] = hexArray[v >>> 4];
         hexChars[j * 2 + 1] = hexArray[v & 0x0F];
      }
      return new String(hexChars);
   }


}
