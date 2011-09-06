package com.spidertracks.datanucleus.model;

import java.io.Serializable;

/**
 * Test composite key for the key mapper
 * @author Todd Nine
 *
 */
public class CompositeKey implements Serializable{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    
    private long first;
    private long second;
    private int third;
    
    
    
    public CompositeKey() {
        super();
    }
    

    public CompositeKey(long first, long second, int third) {
        super();
        this.first = first;
        this.second = second;
        this.third = third;
    }
    /**
     * @return the first
     */
    public long getFirst() {
        return first;
    }
    /**
     * @param first the first to set
     */
    public void setFirst(long first) {
        this.first = first;
    }
    /**
     * @return the second
     */
    public long getSecond() {
        return second;
    }
    /**
     * @param second the second to set
     */
    public void setSecond(long second) {
        this.second = second;
    }
    /**
     * @return the third
     */
    public int getThird() {
        return third;
    }
    /**
     * @param third the third to set
     */
    public void setThird(int third) {
        this.third = third;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (first ^ (first >>> 32));
        result = prime * result + (int) (second ^ (second >>> 32));
        result = prime * result + third;
        return result;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CompositeKey other = (CompositeKey) obj;
        if (first != other.first)
            return false;
        if (second != other.second)
            return false;
        if (third != other.third)
            return false;
        return true;
    }

    
}