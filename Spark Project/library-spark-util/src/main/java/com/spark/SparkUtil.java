package com.meras.iot.core.spark;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meras.iot.core.config.HostProperties;
import com.meras.iot.core.spark.been.SparkMetaInfo;

public class SparkUtil {
	private static final Logger logger = LoggerFactory.getLogger(SparkUtil.class);

	public static Builder getSparkBuilder(String appname) {

		Builder spark = SparkSession.builder().master(HostProperties.getSparkServerHostIPs()).appName(appname);
		return spark;
	}

	public static Dataset<Row> getSqlQueryvalueWithFetchSize(SparkSession spark, String query, int fetchsize) {
		Dataset<Row> jdbcDF = buildAndGetJDBCConnection(spark).option("query", query).option("fetchsize", fetchsize).load();
		return jdbcDF;
	}

	public static DataFrameReader buildAndGetJDBCConnection(SparkSession spark) {
		String url = HostProperties.getPostgresqlURL();
		String user = HostProperties.getPGHostUsername();
		String password = HostProperties.getPGHostPassword();
		DataFrameReader jdbcDF = spark.read().format("jdbc").option("url", url).option("user", user).option("password", password);
		return jdbcDF;
	}

	public static void saveOnAvaroFile(SparkSession spark, Set<String> dates, SparkMetaInfo sparkMetaInfo) {

		for (String date : dates) {
			String query = String.format(sparkMetaInfo.getQuery(), date);
			String fileNAme = BatchProcessorUtil.getAvaroFileName(sparkMetaInfo, date);

			Dataset<Row> avaroFileDS = buildAndGetJDBCConnection(spark).option("query", query).option("fetchsize", sparkMetaInfo.getFetchsize()).load();
			try {
				avaroFileDS.write().format("avro").mode(SaveMode.Overwrite).save(fileNAme);
				logger.info("??? ", keyValue("query", query),keyValue("fileNAme", fileNAme));

			}
			catch (Exception e) {
				logger.error("AvaroFileException", keyValue("exception", e), keyValue("query", query));
			}
		}

	}

	public static SparkConf getESSparkConfig() {
		SparkConf conf = new SparkConf();
//		conf.set("es.nodes", HostProperties.getESHostIPs()[0]);
//		conf.set("es.port", String.valueOf(HostProperties.getESHostPort()));
//		conf.set("es.nodes.wan.only", "false");
//		conf.set("es.nodes.discovery", "false");
//		conf.set("es.input.use.sliced.partitions", "false");
//		conf.set("es.net.http.auth.user", HostProperties.getESAuthencationUsername());
//        conf.set("es.net.http.auth.pass", HostProperties.getESAuthencationPassword());
		conf.set("es.nodes", "127.0.0.1");
		conf.set("es.port", "9200");
		conf.set("es.nodes.wan.only", "false");
		conf.set("es.nodes.discovery", "false");
		conf.set("es.input.use.sliced.partitions", "false");
		conf.set("es.write.operation", "upsert");

		return conf;
	}

	public static SparkConf getS3SparkConfig() {
		SparkConf conf = new SparkConf();

		conf.set("fs.s3a.access.key", "AKIAJ3W7OZDG767GEOJQ");
		conf.set("fs.s3a.secret.key", "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");
		conf.set("region", "ap-south-1");

		return conf;
	}

	public static SparkMetaInfo getEsJSONMapper(String esJSOn) {
		SparkMetaInfo esMetaInfo = null;
		try {
			ObjectMapper objectMapper = new ObjectMapper();

			esMetaInfo = objectMapper.readValue(esJSOn, SparkMetaInfo.class);
			return esMetaInfo;

		}
		catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return esMetaInfo;
	}

}
