package com.meras.iot.core.spark;

import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.meras.iot.core.config.PlatformProperties;
import com.meras.iot.core.spark.been.SparkMetaInfo;
import com.meras.iot.core.util.constants.ResourceConstants;

public class BatchProcessorUtil {
	public static final DateTimeFormatter t2 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

	public static String getNextDate(String date) {

		DateTime dateTime = DateTime.parse(date, ResourceConstants.DATE_FORMATTER_YYYY_HYPHEN_MM_HYPHEN_DD).plusDays(1);
		return dateTime.toString(ResourceConstants.DATE_FORMAT_YYYY_HYPHEN_MM_HYPHEN_DD);
	}

	public static String gettDate(String date) {

		DateTime dt = DateTime.parse(date, t2);
		return dt.toString(ResourceConstants.DATE_FORMAT_YYYY_HYPHEN_MM_HYPHEN_DD);
	}

	public static Set<String> getDateList(String spiltedDate) {
		Set<String> dateLists = new HashSet<String>();
		String[] dates = spiltedDate.split(",");
		for (String date : dates) {

			if (date.contains(":")) {
				String[] s2 = date.split(":");
				String startDate = s2[0];
				String endDate = s2[1];
				while (!endDate.equals(startDate)) {
					dateLists.add(startDate);
					startDate = BatchProcessorUtil.getNextDate(startDate);
				}
				dateLists.add(endDate);
			}
			else {
				dateLists.add(date);

			}
		}
		return dateLists;
	}

	public static String getAnaylitcQuery(String query, String startDate) {
		return String.format(query, startDate);
	}

	public static String getAvaroFileName(SparkMetaInfo sparkMetaInfo, String date) {
		String[] a2 = date.split("-");
		return String.format("%s/%s/%s/%s/%s", sparkMetaInfo.getBuckedName(), sparkMetaInfo.getObjcat(), a2[0], a2[1], date.replaceAll("-", ""));
	}

	public static String getIndexName(String objcat, String tenantId, String Date) {
		String index = PlatformProperties.getIndex(objcat, tenantId, Date);

		return String.format("%s/docs", index);
	}

}
