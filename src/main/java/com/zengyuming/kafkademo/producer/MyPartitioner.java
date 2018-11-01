package com.zengyuming.kafkademo.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import javax.management.relation.InvalidRelationIdException;
import java.util.List;
import java.util.Map;

public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if ((keyBytes == null) || (!(key instanceof String))) {
            //throw new InvalidRelationIdException("We expect all messages to have customer name as key");
            return 0;
        }
        if (((String) key).equals("Martin.Zeng")) {
            return numPartitions;
        } else {
            return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1));
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}