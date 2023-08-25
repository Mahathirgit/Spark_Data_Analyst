package com.meras.iot.core.spark.been;
import com.fasterxml.jackson.databind.ser.std.SerializableSerializer;
 
public class Mapper extends SerializableSerializer {
	private String source_field;
	private String target_field;

	public String getSource_field() {
		return source_field;
	}

	public void setSource_field(String source_field) {
		this.source_field = source_field;
	}

	public String getTarget_field() {
		return target_field;
	}

	public void setTarget_field(String target_field) {
		this.target_field = target_field;
	}

}
