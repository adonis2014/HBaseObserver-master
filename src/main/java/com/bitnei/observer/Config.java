package com.bitnei.observer;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class Config {
	// ElasticSearch的集群名称
	public static final String CLUSTER_NAME = "es-test";
	// ElasticSearch的host
	public static final String NODE_HOSTS = "192.168.1.104,192.168.1.105,192.168.1.106";
	// ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
	public static final int NODE_PORT = 9300;
	// ElasticSearch的索引名称
	static String indexName = "test";
	// ElasticSearch的类型名称
	static String typeName = "doc";

	public static String getInfo() {
		List<String> fields = new ArrayList<String>();
		try {
			for (Field f : Config.class.getDeclaredFields()) {
				fields.add(f.getName() + "=" + f.get(null));
			}
		} catch (IllegalAccessException ex) {
			ex.printStackTrace();
		}
		return StringUtils.join(fields, ", ");
	}
}
