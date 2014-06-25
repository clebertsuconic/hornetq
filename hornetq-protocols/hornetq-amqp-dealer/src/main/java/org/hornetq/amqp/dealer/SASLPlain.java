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

public class SASLPlain extends SASL
{
   private String username;
   private String password;

   public SASLPlain(String user, String password)
   {
      this.username = user;
      this.password = password;
   }

   public String getName()
   {
      return "PLAIN";
   }

   public byte[] getBytes()
   {

      if (username == null)
      {
         username = "";
      }

      if (password == null)
      {
         password = "";
      }

      byte[] usernameBytes = username.getBytes();
      byte[] passwordBytes = password.getBytes();
      byte[] data = new byte[usernameBytes.length + passwordBytes.length + 2];
      System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
      System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);
      return data;
   }


}
