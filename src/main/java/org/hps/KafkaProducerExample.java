package org.hps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    private static Instant start = null;
    private static long iteration = 0;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        Random rnd = new Random();
        Workload wrld = new Workload();
        KafkaProducerConfig config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        KafkaProducer<String, Customer> producer = new KafkaProducer<String,Customer>(props);
        log.info("Sending {} messages ...", config.getMessageCount());
        boolean blockProducer = System.getenv("BLOCKING_PRODUCER") != null;
        AtomicLong numSent = new AtomicLong(0);
        // over all the workload
        long key = 0L;
        for (int i = 0; i < wrld.getDatax().size(); i++) {

            //while(true){
            log.info("sending a batch of authorizations of size:{}",
                    Math.ceil(wrld.getDatay().get(i)));
            //   loop over each sample
            for (long j = 0; j < Math.ceil(wrld.getDatay().get(i)); j++) {

                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                Future<RecordMetadata> recordMetadataFuture =
                        producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                                null, null, String.valueOf(key)  ,  custm));
                log.info("Sending the following key {} with the following customer{}", key, custm.toString());
                key++;
                if (blockProducer) {
                    try {
                        recordMetadataFuture.get();
                        // Increment number of sent messages only if ack is received by producer
                        numSent.incrementAndGet();
                    } catch (ExecutionException e) {
                        log.warn("Message {} wasn't sent properly!", e.getCause());
                    }
                } else {
                    // Increment number of sent messages for non blocking producer
                    numSent.incrementAndGet();
                }
                log.info("sleeping for {} seconds", delay);
            }
            Thread.sleep(delay);
        }
    }
}
