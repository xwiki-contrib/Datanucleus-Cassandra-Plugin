/**
 * 
 */
package com.spidertracks.datanucleus.basic.model;

import javax.jdo.annotations.Index;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

import com.spidertracks.datanucleus.model.BaseEntity;



/**
 * @author Todd Nine
 * Basic object for testing primitive persistence.  Only support types in java.lang
 * 
 */
@PersistenceCapable(table = "PrimitiveObject")
public class PrimitiveObject extends BaseEntity{



	@Persistent
	private boolean testBool;

	@Persistent
	private short testShort;

	@Persistent
	private int testInt;

	@Persistent
	private long testLong;

	@Persistent
	private double testDouble;

	@Persistent
	private float testFloat;

	@Persistent
	private byte testByte;

	@Persistent
	private char testChar;

	@Persistent
	@Index
	private String testString;

	@Persistent
	private String nonIndexedString;

	public String getNonIndexedString() {
		return nonIndexedString;
	}

	public void setNonIndexedString(String nonIndexedString) {
		this.nonIndexedString = nonIndexedString;
	}

	public boolean isTestBool() {
		return testBool;
	}

	public void setTestBool(boolean testBool) {
		this.testBool = testBool;
	}

	public short getTestShort() {
		return testShort;
	}

	public void setTestShort(short testShort) {
		this.testShort = testShort;
	}

	public int getTestInt() {
		return testInt;
	}

	public void setTestInt(int testInt) {
		this.testInt = testInt;
	}

	public long getTestLong() {
		return testLong;
	}

	public void setTestLong(long testLong) {
		this.testLong = testLong;
	}

	public double getTestDouble() {
		return testDouble;
	}

	public void setTestDouble(double testDouble) {
		this.testDouble = testDouble;
	}

	public float getTestFloat() {
		return testFloat;
	}

	public void setTestFloat(float testFloat) {
		this.testFloat = testFloat;
	}

	public byte getTestByte() {
		return testByte;
	}

	public void setTestByte(byte testByte) {
		this.testByte = testByte;
	}

	public char getTestChar() {
		return testChar;
	}

	public void setTestChar(char testChar) {
		this.testChar = testChar;
	}

	public String getTestString() {
		return testString;
	}

	public void setTestString(String testString) {
		this.testString = testString;
	}


}
