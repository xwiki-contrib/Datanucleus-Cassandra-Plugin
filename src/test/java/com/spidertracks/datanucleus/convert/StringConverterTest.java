/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
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

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * @author Todd Nine
 * 
 */
public class StringConverterTest {

    /**
     * Test method for
     * {@link com.spidertracks.datanucleus.convert.StringConverter#getObject(org.scale7.cassandra.pelops.Bytes)}
     * .
     * 
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testGetObject() throws UnsupportedEncodingException {
        String value = "foo";

        StringConverter converter = new StringConverter();

        ByteBuffer buffer = ByteBuffer.allocate(3);

        buffer.mark();
        buffer.put(value.getBytes("UTF-8"));
        buffer.reset();

        String returned = (String) converter.getObject(buffer, null);

        assertEquals(value, returned);
    }

    /**
     * Test method for
     * {@link com.spidertracks.datanucleus.convert.StringConverter#getBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testGetBytes() {
        String string = "foo";
        StringConverter converter = new StringConverter();

        Bytes bytes = Bytes.fromUTF8(string);

        ByteBuffer buffer = converter.writeBytes(string, null, null);
        buffer.reset();

        byte[] data = new byte[buffer.limit() - buffer.position()];
        buffer.get(data);

        assertArrayEquals(bytes.toByteArray(), data);
    }

    /**
     * Test method for
     * {@link com.spidertracks.datanucleus.convert.StringConverter#getBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testGetBytesNull() {
        String string = null;

        StringConverter converter = new StringConverter();

        ByteBuffer buffer = converter.writeBytes(string, null, null);

        assertNull(buffer);
    }

    /**
     * Test method for
     * {@link com.spidertracks.datanucleus.convert.StringConverter#getComparatorType()}
     * .
     */
    @Test
    public void testGetComparatorType() {
        assertEquals(ColumnFamilyManager.CFDEF_COMPARATOR_UTF8,
                new StringConverter().getComparatorType());
    }

}
