package kafkatest.issues;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.tools.ConsumerOffsetChecker;

/**
 * Hello world!
 *
 */
public class KafkaProducer extends Thread {
	private final Producer<String, String> producer;
	public final static String TOPIC = "test12";//"test-rep-one";
	//public final static String KAFKAIP = "172.28.18.45:9092,172.28.18.46:9092,172.28.18.47:9092";
	public final static String KAFKAIP ="172.28.20.103:9092,172.28.20.104:9092,172.28.20.105:9092";
			
	private KafkaProducer() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		props.put("metadata.broker.list", KAFKAIP);

		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		// request.required.acks
		// 0, which means that the producer never waits for an acknowledgement
		// from the broker (the same behavior as 0.7). This option provides the
		// lowest latency but the weakest durability guarantees (some data will
		// be lost when a server fails).
		// 1, which means that the producer gets an acknowledgement after the
		// leader replica has received the data. This option provides better
		// durability as the client waits until the server acknowledges the
		// request as successful (only messages that were written to the
		// now-dead leader but not yet replicated will be lost).
		// -1, which means that the producer gets an acknowledgement after all
		// in-sync replicas have received the data. This option provides the
		// best durability, we guarantee that no messages will be lost as long
		// as at least one in sync replica remains.
		props.put("request.required.acks", "-1");

		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	void produce() throws InterruptedException {
		int messageNo = 201;
		final int COUNT = 3000;

		while (messageNo < COUNT) {
			String key = this.getName() + "--" + String.valueOf(messageNo);
			String data = "hello kafka message " + key;
			producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
			System.out.println(data);
			Thread.sleep(1000);
			
			messageNo++;
		}
	}

	public void run() {
		try {
			produce();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		KafkaProducer t1 = new KafkaProducer();
		KafkaProducer t2 = new KafkaProducer();
		t1.start();
//		t2.start();
		
	}
}