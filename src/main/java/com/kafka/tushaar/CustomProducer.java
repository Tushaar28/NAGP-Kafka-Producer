package com.kafka.tushaar;

import java.util.Properties;
import java.io.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomProducer {
	private final static String TOPIC = "tushaar";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    
    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        //props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }
    
    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<String, String> producer = createProducer();
        
        File file = new File("D://file.txt");
    	FileReader fr=new FileReader(file);
    	BufferedReader br=new BufferedReader(fr);
        try {
        	String line;  
        	int index = 0;
        	while((line=br.readLine())!=null) {
        		System.out.println(line);
        		long time = System.currentTimeMillis();
        		String key = String.valueOf(time) + "+" + String.valueOf(index);
        		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, line);
        		producer.send(record);
        		System.out.println("Message sent to topic");
        		index++;
        	}
        }  catch (Exception e) {
        	System.out.println("ERROR CLASS = " + e.getClass());
        	System.out.println("ERROR = " + e.getMessage());
        }finally {
        	br.close();
            producer.flush();
            producer.close();  
        }
    }
    
public static void main(String[] args) throws Exception {
    	runProducer(2);
    }

}
