/**********************************************************************
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.convert;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * A ByteConverter which can convert an array of anything for which there is an exiting converter.
 * This converter places a 1, 3, or 6 byte value behind each element in the array to indicate the length
 * of the next element.
 *
 * A null value for an array corrisponds to no content written.
 * An empty array corrisponds to a single 0xFF byte.
 * Everything else is represented as:
 * length|value|length|value|length|value
 * The length of a length entry is variable but the end point can be determined by reading it.
 * see: {@link writeLength(int, ByteBuffer)} for more information about length encoding.
 *
 * @version $Id$
 * @since 1.1.1-0.7.0
 */
public class ArrayConverter implements ByteConverter
{
    /** The converter for the type of object which this converter handles an array of. */
    private final ByteConverter innerConverter;

    /**
     * An empty array which is the proper type for this array converter.
     * This is the type of array returned by {@link #getObject(ByteBuffer, ByteConverterContext)}.
     */
    private final Object[] arrayInstance;

    /**
     * The Constructor.
     *
     * @param innerConverter the converter for the type of object which this converter handles an array of.
     * @param innerType the class of object which this converter handles an array of.
     */
    public ArrayConverter(final ByteConverter innerConverter, final Class<?> innerType)
    {
        this.innerConverter = innerConverter;
        this.arrayInstance = (Object[]) Array.newInstance(innerType, 0);
    }

    /**
     * {@inheritDoc}
     *
     * @see ByteConverter#getObject(ByteBuffer, ByteConverterContext)
     */
    @Override
    public Object getObject(final ByteBuffer buffer, final ByteConverterContext context)
    {
        // No content corrisponds to null.
        if (buffer == null || buffer.remaining() == 0) {
            return null;
        }

        // Only one byte, if it is 0xFF then this is an empty array,
        // a single 0xFF byte is an invalid length.
        if (buffer.remaining() == 1) {
            buffer.mark();
            if ((buffer.get() & 0xFF) == 0xFF) {
                return this.arrayInstance;
            }
            buffer.reset();
        }

        List<Object> out = new ArrayList<Object>();
        int originalLimit = buffer.limit();
        int next = buffer.position();
        for (;;) {
            if (originalLimit == next) {
                // Reached the limit of the buffer, we're done.
                break;
            }

            int length = readLength(buffer);

            next = buffer.position() + length;
            try {
                buffer.limit(next);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Could not parse input for entry #" + (out.size() + 1) + " in "
                                           + "array. (failed to reset buffer limit from " + buffer.limit()
                                           + " to " + next + " this probably means the content was "
                                           + "altered.\nThe first " + out.size() + " entries were " + out);
            }

            out.add(this.innerConverter.getObject(buffer, context));

            buffer.limit(originalLimit);
            buffer.position(next);
        }

        return out.toArray(this.arrayInstance);
    }

    /**
     * {@inheritDoc}
     *
     * @see ByteConverter#writeBytes(Object, ByteBuffer, ByteConverterContext)
     */
    @Override
    public ByteBuffer writeBytes(final Object value,
                                 final ByteBuffer buffer,
                                 final ByteConverterContext context)
    {
        if (value == null) {
            return buffer;
        }

        final Object[] values = (Object[]) value;

        if (values.length == 0) {
            return ConverterUtils.check(buffer, 1).put((byte) 0xFF);
        }

        ByteBuffer tmpBuff = ByteBuffer.allocate(65535);
        ByteBuffer out = buffer;

        for (int i = 0; i < values.length; i++) {
            // Need to mark the temp buffer because check() uses reset().
            tmpBuff.mark();

            tmpBuff = this.innerConverter.writeBytes(values[i], tmpBuff, context);
            int length = tmpBuff.position();

            // Check 7 bytes + length because 7 is the
            // greatest number of bytes it can take to represent a length.
            out = ConverterUtils.check(out, length + 7);

            writeLength(length, out);

            // Copy the content from the temp buffer to the out buffer.
            tmpBuff.flip();
            out.put(tmpBuff);
            tmpBuff.clear();
        }

        // Set the limit of this buffer at the current location.
        out.limit(out.position());

        return out;
    }

    /**
     * {@inheritDoc}
     *
     * @see ByteConverter#getComparatorType()
     */
    @Override
    public String getComparatorType()
    {
        return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
    }

    /* --------------------- Stateless. --------------------- */

    /**
     * Write the length in a space efficient way.
     * For lengths less than 255 bytes long, one byte is used.
     * For lengths between 255 and 65534, 3 bytes are used.
     * For lengths between 65534 and 2,147,483,648 (Integer.MAX_VALUE), 7 bytes are used.
     * The encoding scheme for entries less than 254 bytes long is:
     * <length (1 byte)> content
     * For less than 65535:
     * 0xFF <length (2 bytes)> content
     * For longer:
     * 0xFF 0xFF 0xFF <length (4 bytes)> content
     *
     * @param length the length of the content which will be written next.
     * @param writeTo the buffer to write the encoded length to.
     */
    private static void writeLength(final int length, final ByteBuffer writeTo)
    {
        if (length < 255) {
            writeTo.put((byte) length);
            return;
        }

        writeTo.put((byte) 0xFF);

        if (length < 65535) {
            writeTo.putShort((short) length);
            return;
        }
        writeTo.putShort((short) 0xFFFF).putInt(length);
    }

    /**
     * Read a length of a piece of data as generated by {@link #writeLength(int, ByteBuffer)}.
     *
     * @param readFrom the ByteBuffer to read the data from.
     * @return the unpacked length.
     */
    private static int readLength(final ByteBuffer readFrom)
    {
        // using bitwise and to prevent signedness of the byte.
        int value = readFrom.get() & 0xFF;

        if (value != 0xFF) {
            return value;
        }

        value = readFrom.getShort() & 0xFFFF;
        if (value != 0xFFFF) {
            return value;
        }

        return readFrom.getInt();
    }
}
