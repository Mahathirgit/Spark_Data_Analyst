package com.meras.iot.core.spark.esbuilder.json.file;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import com.meras.iot.core.spark.been.SparkMetaInfo;
import com.meras.iot.core.spark.transformer.CoreMapper;

public class ESRequestBuilderForJsonFile implements MapFunction<Row, String> {

	private static final long serialVersionUID = 1L;
	SparkMetaInfo esMetaInfo;

	public ESRequestBuilderForJsonFile(SparkMetaInfo esMetaInfo) {
		this.esMetaInfo = esMetaInfo;
	}

	@Override
	public String call(Row value) throws Exception {
		JSONObject data = null;
		try {
			data = CoreMapper.getJsonToCoreData(value.json(), esMetaInfo);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return data.toString();
	}
}
