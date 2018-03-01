package com.maxbin.hadoop.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	
	private String phoneNB;
	private long u_flow;
	private long d_flow;
	private long s_flow;
	
	//在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参构造函数
	public FlowBean(){}
	
	//构造函数，为了对象数据初始化方便，加入一个带参的构造函数
	public FlowBean(String phoneNB, long up_flow, long d_flow) {
		super();
		this.phoneNB = phoneNB;
		this.u_flow = up_flow;
		this.d_flow = d_flow;
		this.s_flow = up_flow + d_flow;
	}
	
	public void set(String phoneNB, long up_flow, long d_flow) {
		this.phoneNB = phoneNB;
		this.u_flow = up_flow;
		this.d_flow = d_flow;
		this.s_flow = up_flow + d_flow;
	}

	public String getPhoneNB() {
		return phoneNB;
	}

	public void setPhoneNB(String phoneNB) {
		this.phoneNB = phoneNB;
	}

	public long getU_flow() {
		return u_flow;
	}

	public void setU_flow(long up_flow) {
		this.u_flow = up_flow;
	}

	public long getD_flow() {
		return d_flow;
	}

	public void setD_flow(long d_flow) {
		this.d_flow = d_flow;
	}

	public long getS_flow() {
		return s_flow;
	}

	public void setS_flow(long s_flow) {
		this.s_flow = s_flow;
	}
	
	//将对象数据序列化到流中
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(phoneNB);
		out.writeLong(u_flow);
		out.writeLong(d_flow);
		out.writeLong(s_flow);
		
	}
	
	//从数据流中反序列化出对象的数据
	//从数据流中读出对象字段使，必须跟序列化时的顺序一致
	public void readFields(DataInput in) throws IOException {
		
		phoneNB = in.readUTF();
		u_flow = in.readLong();
		d_flow = in.readLong();
		s_flow = in.readLong();
		
	}
	
	@Override
	public String toString() {
		return "" + u_flow + "\t" + d_flow + "\t" + s_flow;
	}
	

	public int compareTo(FlowBean o) {
		
		return s_flow > o.getS_flow()?-1:1;
	}

}
