package com.meras.iot.core.spark.been;

import java.util.List;

import org.json.JSONArray;

import com.fasterxml.jackson.databind.ser.std.SerializableSerializer;

public class SparkMetaInfo extends SerializableSerializer {
	private String columnames;
	private String appName;
	private String objcat;
	private String tenantId;
	private String fileName;
	private String methodType;
	private String buckedName;
	private String query;
	private String dates;
	private List<Mapper> mapperArray;
	private TypeA typeA;

	public String getObjcat() {
		return objcat;
	}

	public void setObjcat(String objcat) {
		this.objcat = objcat;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public String getDates() {
		return dates;
	}

	public TypeA getTypeA() {
		return typeA;
	}

	public void setTypeA(TypeA typeA) {
		this.typeA = typeA;
	}

	public String getMethodType() {
		return methodType;
	}

	public void setMethodType(String methodType) {
		this.methodType = methodType;
	}

	public String getBuckedName() {
		return buckedName;
	}

	public void setBuckedName(String buckedName) {
		this.buckedName = buckedName;
	}

	public void setDates(String dates) {
		this.dates = dates;
	}

	private Integer fetchsize;

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Integer getFetchsize() {
		return fetchsize;
	}

	public void setFetchsize(Integer fetchsize) {
		this.fetchsize = fetchsize;
	}

	public String getColumnames() {
		return columnames;
	}

	public List<Mapper> getMapperArray() {
		return mapperArray;
	}

	public void setMapperArray(List<Mapper> mapperArray) {
		this.mapperArray = mapperArray;
	}

	public void setColumnames(String columnames) {
		this.columnames = columnames;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String avroFileName) {
		this.fileName = avroFileName;
	}

}
