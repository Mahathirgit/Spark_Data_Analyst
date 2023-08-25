package com.meras.iot.core.export.json.file.to.es.batch.processor;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.meras.iot.core.spark.BatchProcessorUtil;
import com.meras.iot.core.spark.SparkUtil;
import com.meras.iot.core.spark.been.SparkMetaInfo;
import com.meras.iot.core.spark.esbuilder.ESRequestBuilderForAvroFile;
import com.meras.iot.core.spark.esbuilder.json.file.ESRequestBuilderForJsonFile;
import com.meras.iot.core.util.constants.ResourceConstants;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class ExportJSONFileTOESBatchProcessor {
	public static void main(String[] args) {

//		String esJSON=args[0];

		try {
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader("/usr/local/config/export-json-to-es-batch-processor.json"));
			
			
			SparkMetaInfo esMetaInfo = SparkUtil.getEsJSONMapper(obj.toString());
			String objcat = esMetaInfo.getObjcat();
			String tenantId = esMetaInfo.getTenantId();
			String date = esMetaInfo.getDates();

			SparkSession spaekSession = SparkUtil.getSparkBuilder(esMetaInfo.getAppName()).config(SparkUtil.getESSparkConfig()).getOrCreate();
			Dataset<Row> indexDataset = spaekSession.read().json(esMetaInfo.getFileName());
			
			JavaRDD<String> bulkESrequest = buildEsValue(esMetaInfo, indexDataset);
			String indexName = "test1234";
			Map<String, String> esMApping = new HashMap<String, String>();
			esMApping.put("es.mapping.id", "id");
//			esMApping.put("es.mapping.routing", "account.id");
			JavaEsSpark.saveJsonToEs(bulkESrequest,indexName, esMApping);

		}
		catch (FileNotFoundException | ParseException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static JavaRDD<String> buildEsValue(SparkMetaInfo esMetaInfo, Dataset<Row> indexDataset) {
		Encoder<String> stringEncoder = Encoders.STRING();
		JavaRDD<String> bulkESrequest = indexDataset.map(new ESRequestBuilderForJsonFile(esMetaInfo), stringEncoder).toJavaRDD();
		return bulkESrequest;
	}


}
