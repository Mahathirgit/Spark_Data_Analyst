package com.meras.iot.core.export.es.to.file.batch.processor;

import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.json.JSONObject;

import scala.Tuple2;

public class EventMapper implements Function<Tuple2<String,Map<String,Object>>, String>{

	@Override
	public String call(Tuple2<String, Map<String, Object>> value) throws Exception {
		JSONObject event=new JSONObject(value._2);
		return event.toString();
	}

	


	

}
