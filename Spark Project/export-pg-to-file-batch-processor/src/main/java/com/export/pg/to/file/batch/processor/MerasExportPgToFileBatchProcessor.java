package com.meras.iot.core.export.pg.to.file.batch.processor;
import static net.logstash.logback.argument.StructuredArguments.keyValue;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.SparkSession;

import com.meras.iot.core.export.pg.to.file.batch.processor.d1type.D1Type;
import com.meras.iot.core.export.pg.to.file.batch.processor.util.CmdUtil;
import com.meras.iot.core.spark.BatchProcessorUtil;
import com.meras.iot.core.spark.SparkUtil;
import com.meras.iot.core.spark.been.SparkMetaInfo;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class MerasExportPgToFileBatchProcessor {
	private static final Logger logger = LoggerFactory.getLogger(MerasExportPgToFileBatchProcessor.class);

	public static void main(String[] args) {
		try {
			logger.info("Initializing Processor ", keyValue("Dates", CmdUtil.getDates(args)),keyValue("File_Path", CmdUtil.getFilePath(args)));

			SparkMetaInfo sparkMetaInfo = getMetaInfoFromArgument(args);
			Set<String> dates = BatchProcessorUtil.getDateList(CmdUtil.getDates(args));

			SparkSession spark = SparkUtil.getSparkBuilder(sparkMetaInfo.getAppName()).config(SparkUtil.getS3SparkConfig()).getOrCreate();

			if ("D1Type".equals(sparkMetaInfo.getMethodType())) {

				dates = D1Type.addDataToDataLakeBsedOnDate(spark, sparkMetaInfo, dates);
			}
			logger.info("??? ", keyValue("Dates", dates.toString()));

			SparkUtil.saveOnAvaroFile(spark, dates, sparkMetaInfo);

		}
		catch (FileNotFoundException | ParseException e) {
			logger.error("AvaroFileException", keyValue("exception", e));
		}
	}

	private static SparkMetaInfo getMetaInfoFromArgument(String[] args) throws ParseException, FileNotFoundException {
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(new FileReader(CmdUtil.getFilePath(args)));

		SparkMetaInfo esMetaInfo = SparkUtil.getEsJSONMapper(obj.toString());
		return esMetaInfo;
	}

}
