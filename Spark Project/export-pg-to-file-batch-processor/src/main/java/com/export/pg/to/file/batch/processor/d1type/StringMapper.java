package com.meras.iot.core.export.pg.to.file.batch.processor.d1type;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class StringMapper implements MapFunction<Row, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(Row value) throws Exception {

		String createdState = value.<String>getAs("date");

		return createdState;
	}

}
