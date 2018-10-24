package com.bitnei.observer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.delete.DeleteRequest;

public class DataSyncObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(DataSyncObserver.class);

	/**
	 * 读取HBase Shell的指令参数
	 *
	 * @param env
	 */
	private void readConfiguration(CoprocessorEnvironment env) {
		Configuration conf = env.getConfiguration();
		Config.indexName = conf.get("es_index");
		Config.typeName = conf.get("es_type");

		LOG.info("observer -- started with config: " + Config.getInfo());
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		readConfiguration(env);
		ElasticSearchOperator.init();
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		try {
			String indexId = new String(put.getRow());
			NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
			Map<String, String> json = new HashMap<String, String>();
			for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
				for (Cell cell : entry.getValue()) {
					String key = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					json.put(key, value);
				}
			}
			ElasticSearchOperator.addUpdateBuilderToBulk(ElasticSearchOperator.buildUpdate(indexId, json));
			//LOG.info("observer -- add new doc: " + indexId + " to type: " + Config.typeName);
		} catch (Exception ex) {
			LOG.error(ex);
		}
	}

	@Override
	public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete,
			final WALEdit edit, final Durability durability) throws IOException {
		try {
			String indexId = new String(delete.getRow());
			ElasticSearchOperator.addDeleteBuilderToBulk(new DeleteRequest(Config.indexName, Config.typeName, indexId));
			//LOG.info("observer -- delete a doc: " + indexId);
		} catch (Exception ex) {
			LOG.error(ex);
		}
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		ElasticSearchOperator.close();
	}

	private static void testGetPutData(String rowKey, String columnFamily, String column, String value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
		System.out.println(Bytes.toString(put.getRow()));
		for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
			Cell cell = entry.getValue().get(0);
			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}

	public static void main(String[] args) {
		testGetPutData("111", "doc", "c1", "hello world");
	}
}
