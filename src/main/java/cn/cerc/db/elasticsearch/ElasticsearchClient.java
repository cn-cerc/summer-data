package cn.cerc.db.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkNode;

public class ElasticsearchClient {
    private static final ServerConfig config = ServerConfig.getInstance();
    private static final String prefix = String.format("/%s/%s/elasticsearch/", ServerConfig.getAppProduct(),
            ServerConfig.getAppVersion());

    private static RestHighLevelClient client = null;

    public static RestHighLevelClient getClient() {
        if (client != null)
            return client;
        var host = ZkNode.get()
                .getNodeValue(prefix + "host", () -> config.getProperty("elasticsearch.host", "es.local.top:9200"));
        synchronized (ElasticsearchClient.class) {
            if (client == null)
                client = new RestHighLevelClient(RestClient.builder(HttpHost.create(host)));
        }
        return client;
    }

}
