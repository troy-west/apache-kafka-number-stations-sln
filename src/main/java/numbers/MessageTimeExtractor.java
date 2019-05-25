package numbers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MessageTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        // TODO: Interested to understand why TimestampExtractor.extract is not generic.
        return ((Message) consumerRecord.value()).getTime();
    }
}
