/**
 *
 */
package org.hornetq.utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO: get rid of this
 */
public final class ConfirmationWindowWarning
{
   public final boolean disabled;
   public final AtomicBoolean warningIssued;

   /**
    *
    */
   public ConfirmationWindowWarning(boolean disabled)
   {
      this.disabled = disabled;
      warningIssued = new AtomicBoolean(false);
   }
}
