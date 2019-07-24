package kafka.constants;

public interface IKafkaConstants {
	public static String KAFKA_BROKERS = "adopt.hadoop74:6667, adopt.hadoop82:6667, adopt.hadoop94:6667, adopt.hadoop114:6667, adopt.hadoop120:6667, adopt.hadoop130:6667," +
			"adopt.hadoop8:6667, adopt.hadoop10:6667,adopt.hadoop13:6667,adopt.hadoop14:6667, adopt.hadoop25:6667";
	
	public static Integer MESSAGE_COUNT=1000;
	
	public static String CLIENT_ID="client1";
	
	public static String TOPIC_NAME="demo";
	
	public static String GROUP_ID_CONFIG="consumerGroup10";
	
	public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
	
	public static String OFFSET_RESET_LATEST="latest";
	
	public static String OFFSET_RESET_EARLIER="earliest";
	
	public static Integer MAX_POLL_RECORDS=1;
}
