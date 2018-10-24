package com.bitnei.observer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class ElasticSearchOperator {

	private static final Log LOG = LogFactory.getLog(ElasticSearchOperator.class);
	// 缓冲池最大提交文档数（条）
	private static final int MAX_BULK_ACTIONS = 10000;
	// 缓冲池总文档体积达到多少时提交（MB）
	private static final int MAX_Bulk_Size = 100;
	// 最大提交间隔（秒）
	private static final int MAX_FLUSH_INTERVAL = 30;
	// 并行提交请求数
	private static final int CONCURRENT_REQUESTS = 10;

	private static TransportClient client = null;
	private static BulkProcessor bulkProcessor = null;

	public static void init(){
		Settings settings = Settings.settingsBuilder().put("cluster.name", Config.CLUSTER_NAME)// 设置集群名称
				.put("client.transport.sniff", true).build();// 自动嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中

		client = TransportClient.builder().settings(settings).build();
		String[] nodes = Config.NODE_HOSTS.split(",");
		for (String node : nodes) {
			if (node.length() > 0) {// 跳过为空的node（当开头、结尾有逗号或多个连续逗号时会出现空node）
				String[] hostPort = node.split(":");
				try {
					client.addTransportAddress(
							new InetSocketTransportAddress(InetAddress.getByName(hostPort[0]), Config.NODE_PORT));
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}

		bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				// 提交前调用
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				// 提交结束后调用（无论成功或失败）
				LOG.info("提交" + response.getItems().length + "个文档，用时" + response.getTookInMillis() + "MS"
						+ (response.hasFailures() ? " 有文档提交失败！" : ""));
				if (response.hasFailures()) {
					LOG.warn(response.buildFailureMessage());
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				// 提交结束且失败时调用
				LOG.error("有文档提交失败！after failure=" + failure);
			}
		}).setBulkActions(MAX_BULK_ACTIONS)// 文档数量达到10000时提交
				.setBulkSize(new ByteSizeValue(MAX_Bulk_Size, ByteSizeUnit.MB))// 总文档体积达到100MB时提交
				.setFlushInterval(TimeValue.timeValueSeconds(MAX_FLUSH_INTERVAL))// 每30S提交一次（无论文档数量、体积是否达到阈值）
				.setConcurrentRequests(CONCURRENT_REQUESTS)// 可并行的提交请求数
				.build();
	}

	/**
	 * 构建update
	 *
	 * @param id
	 * @param json
	 */
	public static UpdateRequest buildUpdate(String id, Map<String, String> json) {
		return new UpdateRequest(Config.indexName, Config.typeName, id).doc(json).docAsUpsert(true);
	}

	/**
	 * 加入索引请求到缓冲池
	 *
	 * @param updateRequest
	 */
	public static void addUpdateBuilderToBulk(UpdateRequest updateRequest) {
		bulkProcessor.add(updateRequest);
	}

	/**
	 * 加入删除请求到缓冲池
	 *
	 * @param deleteRequest
	 */
	public static void addDeleteBuilderToBulk(DeleteRequest deleteRequest) {
		bulkProcessor.add(deleteRequest);
	}

	/**
	 * 关闭连接
	 */
	public static void close() {
		try {
			bulkProcessor.awaitClose(8, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
		client.close();
	}

	private static void test() {
		init();
		System.err.println("begin");
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			String a = "{VID:59321919-98e9-41a6-9fc5-c3114a4e7da4,VTYPE:113,2000:20170321100225,3201:1,2301:,2213:1,2201:0,2202:118705,2613:3700,2614:100"
+"00,2615:87,2214:1,2203:14,2204:0,2205:0,2617:9999,2208:0,2209:0,2501:0,2502:117058690,2503:40164779,2001:112,2002:1,2003:MTozMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwX"
+"zMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzM"
+"wMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwX"
+"zMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzMwMF8zMzAwXzMzMDBfMzM"
+"wMF8zMzAwXzMzMDBfMzMwMF8zMzAw,7001:112,7002:1,7003:DOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5Az"
+"kDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQM5AzkDOQ=,2101:12,2102:1,2103:MTo1MV81Ml81MV81M181M181M181Ml81M181MV81Ml81MV81"
+"Mg==,7101:12,7102:1,7103:MzQzNTU1NDUzNDM0,2307:1,2308:MjMwOToxLDIzMTA6MSwyMzAyOjc4LDIzMDM6MjAwNDksMjMxMToyMDAwMCwyMzA0OjY3LDIzMDU6MzY5OSwyMzA2Ojk5OTc=,2601:1,2602:1,2603:3300,2604:1,2605:1,2606:3300,2607:1,2608"
+":4,2609:53,2610:1,2611:1,2612:51,2920:0,3801:0,2921:0,2922:,2804:0,2805:,2923:0,2924:,2808:0,2809:}";
			Map<String, String> json = Utils.processMessageToMap(a);
			addUpdateBuilderToBulk(buildUpdate(String.valueOf(i + 1), json));
		}
		close();
		System.err.println("end\n 用时：" + (System.currentTimeMillis() - begin));
	}

	public static void main(String[] args) {
		test();
	}
}
