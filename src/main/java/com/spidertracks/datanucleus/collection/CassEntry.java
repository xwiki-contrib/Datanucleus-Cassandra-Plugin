package com.spidertracks.datanucleus.collection;

import java.util.Map;

/**
 * Simply entry for returning values to the caller in readmap
 * 
 * @author Todd Nine
 * 
 */
public class CassEntry implements Map.Entry<Object, Object> {

    private Object key;
    private Object value;

    public CassEntry(Object key, Object value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }

}