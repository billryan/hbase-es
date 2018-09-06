package me.yuanbin.bigdata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BackoffPolicy;
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
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by yuanbin on 2018/7/25.
 */
public class HBaseESObserver extends BaseRegionObserver {

    private String esIndexPrefix = null;
    private String esType = null;
    private ES esClient = null;

    private static final byte[] CF_BYTES = Bytes.toBytes("f");
    private static final byte INT_TYPE = (byte)'i';
    private static final byte FLOAT_TYPE = (byte)'f';
    private static final byte DOUBLE_TYPE = (byte)'d';
    private static final byte STRING_TYPE = (byte)'s';
    private static final String STRING_LIST_TYPE = "_css";
    private static final String INT_LIST_TYPE = "_csi";
    private static final String FLOAT_LIST_TYPE = "_csf";
    private static final String DOUBLE_LIST_TYPE = "_csd";

    private static final String eskey = "ES";

    private static final Log LOG = LogFactory.getLog(HBaseESObserver.class);

    // TODO - 支持删除事件

    private static class ES {
        /**
         * do not use static for ES client since HBase RegionServer has it's own classloader for coprocessor
         */
        private TransportClient client = null;
        private BulkProcessor bulkProcessor = null;

        private static final int BULK_COUNT = 50;
        private static final int BULK_SIZE_MB = 5;
        private static final int BULK_FLUSH_INTERVAL = 30;

        public ES(String esHost, int esPort) {
            LOG.info(String.format("create esClient with esHost: %s, esPort: %d...", esHost, esPort));

            try {
                client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), esPort));
                setBulkProcessor();
            } catch (UnknownHostException ex) {
                LOG.error("Unknown Host Exception: " + ex);
            } catch (Exception ex) {
                LOG.error(ex);
            }
        }

        public ES(TransportClient client) {
            this.client = client;
        }

        private void setBulkProcessor() {
            if (client != null) {
                LOG.info("init bulk processor");
                bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) { LOG.debug("before bulk"); }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) { LOG.debug("after bulk response"); }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {

                        LOG.error("after bulk failure");
                        LOG.error(request);
                        LOG.error(failure);
                    }
                })
                        .setBulkActions(BULK_COUNT)
                        .setBulkSize(new ByteSizeValue(BULK_SIZE_MB, ByteSizeUnit.MB))
                        .setFlushInterval(TimeValue.timeValueSeconds(BULK_FLUSH_INTERVAL))
                        .setConcurrentRequests(1)
                        .setBackoffPolicy(
                                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 5))
                        .build();
            } else {
                LOG.error("ES client is not initialized properly!!!");
            }
        }

        public void upsert(String index, String type, String id, Map<String, Object> doc) {
            try {
                bulkProcessor.add(new UpdateRequest()
                        .index(index)
                        .type(type)
                        .id(id)
                        .doc(doc)
                        .docAsUpsert(true)
                );
            } catch (Exception ex) {
                LOG.error("upsert exception");
                LOG.error(ex);
            }
        }

        private void delete(String index, String type, String id) {
            try {
                bulkProcessor.add(new DeleteRequest()
                        .index(index)
                        .type(type)
                        .id(id)
                );
            } catch (Exception ex) {
                LOG.error("delete exception");
                LOG.error(ex);
            }
        }

        private void close() {
            bulkProcessor.close();
            LOG.info("close bulk processor");
            client.close();
            LOG.info("close client");
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("version 0.9, coprocessor received event start");

        Configuration conf = env.getConfiguration();
        String esHost = conf.get("esHost", "localhost");
        int esPort = conf.getInt("esPort", 9300);
        esType = conf.get("esType", "userprofile");

        RegionCoprocessorEnvironment coprocessorEnvironment = (RegionCoprocessorEnvironment)env;
        String table = coprocessorEnvironment.getRegionInfo().getTable().getNameAsString();
        // table = userprofile:userprofile_dev
        String[] splits = table.split(":");
        if (splits.length > 1) {
            esIndexPrefix = conf.get("esIndexPrefix", splits[1]);
        } else {
            esIndexPrefix = conf.get("esIndexPrefix", table);
        }
        LOG.info(String.format("ES config, host: %s, port: %d, index prefix: %s, type: %s", esHost, esPort, esIndexPrefix, esType));

        ConcurrentMap<String, Object> sharedData = coprocessorEnvironment.getSharedData();
        synchronized (sharedData) {
            if (!sharedData.containsKey(eskey)) {
                LOG.info("put with new ES: " + sharedData);
                sharedData.putIfAbsent(eskey, new ES(esHost, esPort));
            }
            try {
                LOG.info("get with new ES: " + sharedData);
                esClient = (ES)sharedData.get(eskey);
            } catch (Exception ex) {
                LOG.error("exception while convert es from shared data to ES");
                LOG.error(ex);
            }
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        LOG.info("coprocessor received event stop");
        ConcurrentMap<String, Object> sharedData = ((RegionCoprocessorEnvironment)env).getSharedData();

        synchronized (sharedData) {
            ES es = (ES)sharedData.get(eskey);
            if (es != null) {
                LOG.info("stop ES: " + es);
                es.close();
                sharedData.remove(eskey);
            }
            esClient = null;
        }
        sharedData = null;
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
                        final Put put, final WALEdit edit, final Durability durability) throws IOException {

        LOG.debug("coprocessor received event postPut");
        String table = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String rowkey = Bytes.toString(put.getRow());
        int keyLen = rowkey.length();
        // TODO - 考虑 extCode 仅为后四位的情况，需要结合末尾四字节和业务需要
        String extCode = (keyLen > 8) ? rowkey.substring(keyLen - 8, keyLen) : "FFFFFFFF";

        Map<String, Object> doc = new HashMap<>();
        Map<byte [], List<Cell>> familyCellMap = put.getFamilyCellMap();
        for (Map.Entry<byte [], List<Cell>> entry : familyCellMap.entrySet()) {
            byte[] cf = entry.getKey();
            if (!Bytes.equals(cf, CF_BYTES)) {
                LOG.info(String.format("do not add column family: %s", Bytes.toString(cf)));
                continue;
            }
            for (Cell cell : entry.getValue()) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                // qualifier = esKey + typeFlag
                String esKey = Bytes.toString(qualifier, 0, qualifier.length - 1);
                byte typeFlag = qualifier[qualifier.length - 1];
                Object esValue = null;
                switch (typeFlag) {
                    case INT_TYPE:
                        esValue = Bytes.toInt(value);
                        break;
                    case FLOAT_TYPE:
                        esValue = Bytes.toFloat(value);
                        break;
                    case DOUBLE_TYPE:
                        esValue = Bytes.toDouble(value);
                        break;
                    case STRING_TYPE:
                        esValue = Bytes.toString(value);
                        // determine if it is a list of string/int/float/double, prefix_csss
                        if (qualifier.length > 5) {
                            String suffix = Bytes.toString(qualifier, qualifier.length - 5, 4);
                            switch (suffix) {
                                case STRING_LIST_TYPE:
                                    esValue = ((String)esValue).split(",");
                                    break;
                                case INT_LIST_TYPE:
                                    esValue = Arrays.stream(((String) esValue).split(",")).map(Integer::parseInt);
                                    break;
                                case FLOAT_LIST_TYPE:
                                    esValue = Arrays.stream(((String) esValue).split(",")).map(Float::parseFloat);
                                    break;
                                case DOUBLE_LIST_TYPE:
                                    esValue = Arrays.stream(((String) esValue).split(",")).map(Double::parseDouble);
                                    break;
                                default:
                                    break;
                            }
                        }
                        break;
                    default:
                        LOG.info("Unsupported type: " + typeFlag);
                        continue;
                }

                long timestamp = cell.getTimestamp();
                doc.put(esKey + ".content", esValue);
                doc.put(esKey + ".updated_at", timestamp);
            }
        }

        try {
            esClient.upsert(esIndexPrefix + "-" + extCode, esType, rowkey, doc);
        } catch (Exception ex) {
            LOG.error("exception while upsert doc to es");
            LOG.error(ex);
        }
    }
}
