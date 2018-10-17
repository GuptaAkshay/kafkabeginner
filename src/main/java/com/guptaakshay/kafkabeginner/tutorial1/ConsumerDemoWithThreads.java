package com.guptaakshay.kafkabeginner.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {

        //create consumer
        new ConsumerDemoWithThreads().run();

    }

    private ConsumerDemoWithThreads(){ }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

        String bootstrapServer = "localhost:9092";
        String groupId= "my_new2_app";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        //creating consumer runnable
        logger.info("Invoking consumer!!");
        Runnable myConsumer = new ConsumerThreads(bootstrapServer, groupId, topic, latch);

        //start a Thread
        Thread myThread = new Thread(myConsumer);
        myThread.start();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Got shoutdown Hook");
            ((ConsumerThreads) myConsumer).shutdown();

            try{
                latch.await();
            }catch (InterruptedException e){
                e.printStackTrace();

            }finally {
                logger.info("Application has exited !!!");
            }

        }));

        try{
            latch.await();
        }catch (InterruptedException e){
            logger.error("Application got interuppted: {}", e);

        }finally {
            logger.info("Application is closed !!!");
        }

    }

    public class ConsumerThreads implements Runnable{

        final Logger logger = LoggerFactory.getLogger(ConsumerThreads.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThreads(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;

            //create consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //getting data from producer
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(" Key: {}, Value: {}", record.key(), record.value());
                        logger.info(" Partitions: {}", record.partition());
                    }
                }
            }catch (WakeupException wex){
                logger.info(" Recieved Shutdown signal! ");
            }
            finally {
                consumer.close();

                //tell main code that we are done
                latch.countDown();
            }
        }

        public void shutdown(){

            //wakeup method is pecial method to intreupt consumer.poll() by throwing exception
            consumer.wakeup();
        }
    }
}
