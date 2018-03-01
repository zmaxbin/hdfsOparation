package com.maxbin.hadoop.twofiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlagString implements Writable{
	
	private String value;
	private int flag;
	
	

	public FlagString() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FlagString(String value, int flag) {
		super();
		this.value = value;
		this.flag = flag;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(flag);
		out.writeUTF(value);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.flag = in.readInt();
		this.value = in.readUTF();
		
	}

}
