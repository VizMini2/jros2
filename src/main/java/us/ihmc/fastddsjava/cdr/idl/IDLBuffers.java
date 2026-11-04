package us.ihmc.fastddsjava.cdr.idl;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

/**
 * Absolute bulk copies between buffers, like {@code put(int, XBuffer, int, int)} (Java 16), which
 * Android only has since API 35: on older phones the sequences failed to grow with a
 * NoSuchMethodError. Overlapping ranges of the same buffer are copied correctly (as when removing an
 * element).
 */
final class IDLBuffers
{
   private IDLBuffers()
   {
   }

   public static void copy(ByteBuffer dst, int dstIndex, ByteBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }

   public static void copy(CharBuffer dst, int dstIndex, CharBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }

   public static void copy(ShortBuffer dst, int dstIndex, ShortBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }

   public static void copy(IntBuffer dst, int dstIndex, IntBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }

   public static void copy(LongBuffer dst, int dstIndex, LongBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }

   public static void copy(FloatBuffer dst, int dstIndex, FloatBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }

   public static void copy(DoubleBuffer dst, int dstIndex, DoubleBuffer src, int srcIndex, int length)
   {
      if (length <= 0)
         return;
      if (dst.hasArray() && src.hasArray())
      {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, dst.array(), dst.arrayOffset() + dstIndex, length);
      }
      else if (dst == src && dstIndex > srcIndex)
      {
         for (int i = length - 1; i >= 0; i--)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
      else
      {
         for (int i = 0; i < length; i++)
            dst.put(dstIndex + i, src.get(srcIndex + i));
      }
   }
}
