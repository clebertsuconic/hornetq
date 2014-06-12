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

package org.hornetq.amqp.dealer.util;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.WritableBuffer;

/**
 * This is a serverMessage that won't deal
 *
 * @author Clebert Suconic
 */
public class ProtonServerMessage
{
   private Header header;
   private DeliveryAnnotations deliveryAnnotations;
   private MessageAnnotations messageAnnotations;
   private Properties properties;
   private ApplicationProperties applicationProperties;

   // This should include a raw body of both footer and body
   private byte[] rawBody;

   private Section parsedBody;
   private Footer parsedFooter;

   private final int EOF = 0;

   // TODO: Enumerations maybe?
   private static final int HEADER_TYPE = 0x070;
   private static final int DELIVERY_ANNOTATIONS = 0x071;
   private static final int MESSAGE_ANNOTATIONS = 0x072;
   private static final int PROPERTIES = 0x073;
   private static final int APPLICATION_PROPERTIES = 0x074;


   /**
    * This will decode a ByteBuffer tha represents the entire message.
    * Set the limits around the parameter.
    *
    * @param buffer a limited buffer for the message
    */
   public void decode(ByteBuffer buffer)
   {

      DecoderImpl decoder = CodecCache.getDecoder();


      header = null;
      deliveryAnnotations = null;
      messageAnnotations = null;
      properties = null;
      applicationProperties = null;
      rawBody = null;

      decoder.setByteBuffer(buffer);
      try
      {
         int type = readType(buffer, decoder);
         if (type == HEADER_TYPE)
         {
            header = (Header) readSection(buffer, decoder);
            type = readType(buffer, decoder);

         }

         if (type == DELIVERY_ANNOTATIONS)
         {
            deliveryAnnotations = (DeliveryAnnotations) readSection(buffer, decoder);
            type = readType(buffer, decoder);

         }

         if (type == MESSAGE_ANNOTATIONS)
         {
            messageAnnotations = (MessageAnnotations) readSection(buffer, decoder);
            type = readType(buffer, decoder);
         }

         if (type == PROPERTIES)
         {
            properties = (Properties) readSection(buffer, decoder);
            type = readType(buffer, decoder);

         }

         if (type == APPLICATION_PROPERTIES)
         {
            applicationProperties = (ApplicationProperties) readSection(buffer, decoder);
            type = readType(buffer, decoder);
         }

         if (type != EOF)
         {
            rawBody = new byte[buffer.limit() - buffer.position()];
            buffer.get(rawBody);
         }
      }
      finally
      {
         decoder.setByteBuffer(null);
      }

   }


   public void encode(ByteBuffer buffer)
   {
      WritableBuffer writableBuffer = new WritableBuffer.ByteBufferWrapper(buffer);
      encode(writableBuffer);
   }

   public void encode(WritableBuffer writableBuffer)
   {
      EncoderImpl encoder = CodecCache.getEncoder();
      encoder.setByteBuffer(writableBuffer);

      try
      {
         if (header != null)
         {
            encoder.writeObject(header);
         }
         if (deliveryAnnotations != null)
         {
            encoder.writeObject(deliveryAnnotations);
         }
         if (messageAnnotations != null)
         {
            encoder.writeObject(messageAnnotations);
         }
         if (properties != null)
         {
            encoder.writeObject(properties);
         }
         if (applicationProperties != null)
         {
            encoder.writeObject(applicationProperties);
         }

         // It should write either the parsed one or the rawBody
         if (parsedBody != null)
         {
            encoder.writeObject(parsedBody);
            if (parsedFooter != null)
            {
               encoder.writeObject(parsedFooter);
            }
         }
         else if (rawBody != null)
         {
            writableBuffer.put(rawBody, 0, rawBody.length);
         }
      }
      finally
      {
         encoder.setByteBuffer((WritableBuffer) null);
      }
   }


   private int readType(ByteBuffer buffer, DecoderImpl decoder)
   {

      int pos = buffer.position();

      if (!buffer.hasRemaining())
      {
         return EOF;
      }
      try
      {
         if (buffer.get() != 0)
         {
            return EOF;
         }
         else
         {
            return ((Number) decoder.readObject()).intValue();
         }
      }
      finally
      {
         buffer.position(pos);
      }
   }


   private Section readSection(ByteBuffer buffer, DecoderImpl decoder)
   {
      if (buffer.hasRemaining())
      {
         return (Section) decoder.readObject();
      }
      else
      {
         return null;
      }
   }
}
