package com.meras.iot.core.spark.esbuilder;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import com.meras.iot.core.spark.been.SparkMetaInfo;
import com.meras.iot.core.spark.transformer.CoreMapper;

public class ESRequestBuilderForAvroFile implements MapFunction<Row, String> {

	private static final long serialVersionUID = 1L;
	SparkMetaInfo esMetaInfo;

	public ESRequestBuilderForAvroFile(SparkMetaInfo esMetaInfo) {
		this.esMetaInfo = esMetaInfo;
	}

	@Override
	public String call(Row value) throws Exception {
		JSONObject data = null;
		try {
			data = CoreMapper.getAvaroToCoreData(value.json(), esMetaInfo);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return data.toString();
	}
}
