package com.meras.iot.core.export.pg.to.file.batch.processor.d1type;

import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.meras.iot.core.spark.BatchProcessorUtil;
import com.meras.iot.core.spark.SparkUtil;
import com.meras.iot.core.spark.been.SparkMetaInfo;
import com.meras.iot.core.spark.been.TypeA;

public class D1Type {
	public static Set<String> addDataToDataLakeBsedOnDate(SparkSession spark,SparkMetaInfo eSMetaInfo,Set<String> dates) {
		try {
			Encoder<String> stringEncoder = Encoders.STRING();

			int fetchsize = eSMetaInfo.getFetchsize();
			TypeA typeA=eSMetaInfo.getTypeA();
			String dateCollectQuery = typeA.getDate_collect_query();

			for (String date : dates) {
				String query = BatchProcessorUtil.getAnaylitcQuery(dateCollectQuery, date);
				Dataset<String> tablevalueDS = SparkUtil.getSqlQueryvalueWithFetchSize(spark, query, fetchsize).map(new StringMapper(), stringEncoder);
				dates.addAll(tablevalueDS.collectAsList());

			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return dates;
	}
}
