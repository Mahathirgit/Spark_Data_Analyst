package com.meras.iot.core.export.es.to.file.batch.processor;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.meras.iot.core.config.HostProperties;
import com.meras.iot.core.spark.SparkUtil;
import com.meras.iot.core.spark.been.SparkMetaInfo;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class MerasExportEsToFileBatchProcessor {

	public static void main(String[] args) {

		JSONParser parser = new JSONParser();
		Object obj;
		try {
			obj = parser.parse(new FileReader("/usr/local/config/export-pg-to-file-batch-processor.json"));
			SparkMetaInfo esMetaInfo = SparkUtil.getEsJSONMapper(obj.toString());
			SparkConf conf = SparkUtil.getESSparkConfig().setAppName(esMetaInfo.getAppName()).setMaster(HostProperties.getSparkServerHostIPs());
			JavaSparkContext JavaSparkContext = new JavaSparkContext(conf);
			JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(JavaSparkContext, "charging-session-2023_meras_dev");

			List<String> eventsList = esRDD.map(new EventMapper()).collect();
			Seq<String> eventsSeq = JavaConverters.asScalaIteratorConverter(eventsList.iterator()).asScala().toSeq();

			SparkSession spark = SparkUtil.getSparkBuilder(esMetaInfo.getAppName()).getOrCreate();
			Dataset<String> eventsDataset = spark.createDataset(eventsSeq, Encoders.STRING());
			Dataset<Row> anotherPeople = spark.read().json(eventsDataset);
			anotherPeople.write().json("D:\\T4");
		}
		catch (FileNotFoundException | ParseException e) {
			e.printStackTrace();
		}

	}

}
