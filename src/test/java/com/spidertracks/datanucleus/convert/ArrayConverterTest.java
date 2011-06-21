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

import org.junit.Assert;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * Run tests against {@link ArrayConverter}
 *
 * @version $Id$
 * @since 1.1.1-0.7.0
 */
public class ArrayConverterTest
{
    /**
     * Test method for {@link ArrayConverter#getObject(org.scale7.cassandra.pelops.Bytes)}.
     * Happy path test for getObject.
     */
    @Test
    public void testGetObject()
    {
        final byte[] stored = {
            (byte)5, (byte)'H', (byte)'e', (byte)'l', (byte)'l', (byte)'o',
            (byte)6, (byte)'W', (byte)'o', (byte)'r', (byte)'l', (byte)'d', (byte)'!',
            (byte)2, (byte)':', (byte)')', (byte)0, (byte)0
        };

        final String[] expected = {"Hello", "World!", ":)", "", null};

        final ArrayConverter arrayOfStringConverter =
            new ArrayConverter(new StringConverter(), String.class);

        final ByteBuffer buffer = ByteBuffer.wrap(stored);
        final String[] returned = (String[]) arrayOfStringConverter.getObject(buffer, null);

        Assert.assertEquals(5, returned.length);
        Assert.assertEquals("Hello", returned[0]);
        Assert.assertEquals("World!", returned[1]);
        Assert.assertEquals(":)", returned[2]);

        Assert.assertNull(returned[3]);
        Assert.assertNull(returned[4]);
    }

    /**
     * Prove that the converter knows the difference between an empty array and null.
     */
    @Test
    public void testGetEmptyAndNull()
    {
        final ByteBuffer empty = ByteBuffer.wrap(new byte[] { (byte)0xFF });

        final ArrayConverter arrayOfStringConverter =
            new ArrayConverter(new StringConverter(), String.class);

        Assert.assertNull(arrayOfStringConverter.getObject(null, null));
        Assert.assertNull(arrayOfStringConverter.getObject(ByteBuffer.wrap(new byte[0]), null));
        Assert.assertEquals(0, ((Object[]) arrayOfStringConverter.getObject(empty, null)).length);
    }

    /**
     * Put an array and then get it back.
     */
    @Test
    public void testPutAndGet() throws Exception
    {
        final String[] control = new String(
            "Governments of the Industrial World, you weary giants of flesh and steel, I come from "
          + "Cyberspace, the new home of Mind. On behalf of the future, I ask you of the past to "
          + "leave us alone. You are not welcome among us. "
          + "You have no sovereignty where we gather.").split(" ");

        final ArrayConverter arrayOfStringConverter =
            new ArrayConverter(new StringConverter(), String.class);

        final ByteBuffer bb = arrayOfStringConverter.writeBytes(control, null, null);

        bb.flip();
        final String[] test = (String[]) arrayOfStringConverter.getObject(bb, null);

        Assert.assertEquals(control.length, test.length);
        for (int i = 0; i < control.length; i++) {
            Assert.assertEquals(control[i], test[i]);
        }
    }

    /**
     * Put an array with large elements and then get it back.
     */
    @Test
    public void testPutAndGetLongEntry() throws Exception
    {
        final String[] control = { "Hello", null, "World!", null, ":)", null, "...", "" };

        final StringBuilder sb1 = new StringBuilder(254);
        final StringBuilder sb2 = new StringBuilder(255);
        final StringBuilder sb3 = new StringBuilder(65536);

        for (int i = 0; i < 254; i++) {
            sb1.append('1');
        }
        for (int i = 0; i < 255; i++) {
            sb2.append('2');
        }
        for (int i = 0; i < 65536; i++) {
            sb3.append('3');
        }

        control[1] = sb1.toString();
        control[3] = sb2.toString();
        control[5] = sb3.toString();

        final ArrayConverter arrayOfStringConverter =
            new ArrayConverter(new StringConverter(), String.class);

        final ByteBuffer bb = arrayOfStringConverter.writeBytes(control, null, null);

        bb.flip();
        final String[] test = (String[]) arrayOfStringConverter.getObject(bb, null);

        Assert.assertEquals(control.length, test.length);
        // Can't test last entry since StringConverter makes it in to null.
        for (int i = 0; i < control.length - 1; i++) {
            Assert.assertEquals(control[i], test[i]);
        }
        Assert.assertNull(test[test.length - 1]);
    }
}
