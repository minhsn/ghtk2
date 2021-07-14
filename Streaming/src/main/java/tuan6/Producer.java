package tuan6;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Faker faker=new Faker();

        String topic = "aaa";
        String key = "testkey";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.13:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
//        props.put("schema.registry.url", "http://10.140.0.13:8081");
        org.apache.kafka.clients.producer.Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
        Timestamp now = new Timestamp(55);
        Timestamp then = new Timestamp(now.getTime() + 300000000);


        long offset = Timestamp.valueOf("2019-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2022-01-01 00:00:00").getTime();
        long diff = end - offset + 1;


        int i =500;
        while( i>0) {
            Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
            Data_tracking.DataTracking data_tracking=Data_tracking.DataTracking
                    .newBuilder()
                    .setLon(12)
                    .setLat(11)
                    .setPhoneId(faker.phoneNumber().phoneNumber())
                    .setVersion(String.valueOf(faker.number().randomNumber()))
                    .setName(faker.name().fullName())
                    .setTimestamp(rand.getTime())
                    .build();
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic,null,data_tracking.toByteArray());
            producer.send(record);
            i--;
        }

    }
}
