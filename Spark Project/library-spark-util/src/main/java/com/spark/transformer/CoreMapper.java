package com.meras.iot.core.spark.transformer;

import java.util.List;

import org.json.JSONObject;

import com.meras.iot.core.spark.been.SparkMetaInfo;
import com.meras.iot.core.spark.been.Mapper;
import com.meras.iot.core.util.JSONUtil;
import com.meras.iot.core.util.constants.ResourceConstants;

public class CoreMapper {
	public static JSONObject getAvaroToCoreData(String json, SparkMetaInfo esMetaInfo) {
		JSONObject data = new JSONObject(json);
		JSONObject empty = new JSONObject();

		List<Mapper> Fields = esMetaInfo.getMapperArray();

		for (Mapper Field : Fields) {
			String targetField = Field.getTarget_field();
			if (targetField != null && data.has(Field.getSource_field()) && !data.isNull(Field.getSource_field())) {
				initJson(empty, targetField, data.get(Field.getSource_field()));
			}

		}

		return empty;

	}
	public static JSONObject getJsonToCoreData(String json, SparkMetaInfo esMetaInfo) {
		JSONObject data = new JSONObject(json);
		JSONObject empty = new JSONObject();

		List<Mapper> Fields = esMetaInfo.getMapperArray();
		for (Mapper Field : Fields) {
			String targetField = Field.getTarget_field();
			if (targetField != null && JSONUtil.isNotNullTest(data, Field.getSource_field())) {
				initJson(empty, targetField, JSONUtil.getObject(data, Field.getSource_field()));

			}

		}

		return empty;

	}
	
	public static JSONObject initJson(JSONObject empty, String key, Object value) {
		String keys[] = key.split(ResourceConstants.BACKSLASH + ResourceConstants.DOT);

		for (int i = 0; i <= keys.length - 1; i++) {
			if (empty.isEmpty()) {
				if (1 == keys.length) {
					empty = empty.put(keys[i], value);
				}
				else {
					empty = empty.put(keys[i], new JSONObject());

				}
			}
			else {

				if (keys[i].equals(keys[keys.length - 1])) {
					getKeyObjectInstance(empty, keys, i).put(keys[i], value);
				}
				else {
					if (!getKeyObjectInstance(empty, keys, i).has(keys[i])) {
						getKeyObjectInstance(empty, keys, i).put(keys[i], new JSONObject());

					}
				}

			}
		}
		return empty;
	}

	private static JSONObject getKeyObjectInstance(JSONObject empty, String[] keys, int objectInstaanceLimit) {
		int i = 1;
		JSONObject objectInstaance = empty;
		while (i <= objectInstaanceLimit) {
			objectInstaance = objectInstaance.getJSONObject(keys[i - 1]);
			i++;
		}
		return objectInstaance;
	}

}
