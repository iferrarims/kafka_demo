package kafkatest.issues;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.tools.ConsumerOffsetChecker;
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {

	private final ConsumerConnector consumer;
	//private final static String zk = "172.28.18.49:2181,172.28.18.50:2181,172.28.18.51:2181,172.28.18.52:2181,172.28.18.53:2181";
//	private final static String zk ="172.28.20.98:2181,172.28.20.100:2181,172.28.18.101:2181";
	private final static String zk ="172.28.20.103:2181,172.28.20.104:2181,172.28.18.105:2181";
	
	private final static String group = "test11";

	private KafkaConsumer() {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", zk);
		// props.put("zookeeper.connect","172.28.5.132:3181,172.28.5.131:3181,172.28.5.134:3181");
		// group 代表一个消费组
		props.put("group.id", group);

		// zk连接超时
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("zookeeper.connection.timeout.ms", "15000");
		props.put("auto.commit.interval.ms", "2000");
		// props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ConsumerConfig config = new ConsumerConfig(props);

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	void consume() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));

		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(
				new VerifiableProperties());

		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer
				.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(
				KafkaProducer.TOPIC).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		// System.out.println(it.size());
		int i = 0;
		while (it.hasNext()) {
			System.out.println(it.next().message());
			i++;
		}
		System.out.println(i);
	}

	public static void main(String[] args) {
		String[] arr = { "--zkconnect=" + zk, "--group=" + group };
		//ConsumerOffsetChecker.main(arr);

		new KafkaConsumer().consume();
	}
}