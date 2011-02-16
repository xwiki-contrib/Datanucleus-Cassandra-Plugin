package com.spidertracks.datanucleus.convert;

import java.nio.ByteBuffer;

/**
 * Interface that allows conversions of objects to bytes
 * @author Todd Nine
 *
 */
public interface ByteConverter {

	/**
	 * Read the bytes from the byte buffer and return the object.  Assume the read mark is always set to the correct index as well as limit.
	 * Implementation should always assume that bytes between position and limit need to be read.  This allows the caller (such as association converters)
	 * to set the position and limit.
	 * 
	 * @param buffer
	 * @return
	 */
	public Object getObject(ByteBuffer buffer, ByteConverterContext context);
	
	
	/**
	 * Write the value to the given buffer.  Assume the write mark is always set to the correct index.
	 * The buffer that should be used for the next write operation is returned.  The implementation should be null safe
	 * and should also check the buffer size.  If null is passed the buffer should be allocated.  If the buffer is
	 * too small, a new instance should be allocated and returned with all bytes from the previous buffer.
	 * @param instance
	 * @return
	 */
	public ByteBuffer writeBytes(Object value, ByteBuffer buffer, ByteConverterContext context);
	
	/**
	 * Returns the comparator type (UTF-8, TimeUUIDType etc) that should be used when comparing the values in this column
	 * This is used for validation on secondary indexing.
	 * @return
	 */
	public String getComparatorType();
	
	
	
}
