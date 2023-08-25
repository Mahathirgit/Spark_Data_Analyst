package com.meras.iot.core.spark.been;

import com.fasterxml.jackson.databind.ser.std.SerializableSerializer;

public class TypeA extends SerializableSerializer{
	private String date_collect_query;

	public String getDate_collect_query() {
		return date_collect_query;
	}

	public void setDate_collect_query(String date_collect_query) {
		this.date_collect_query = date_collect_query;
	}

	

}
