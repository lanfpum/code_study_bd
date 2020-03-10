package bd.study.code.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2020/2/20 15:40.
 *
 * 生产者拦截器：用来对消息进行拦截或者修改，也可以用于Producer的Callback回调之前进行相应的预处理
 */
public class ProducerInterceptorDemo implements ProducerInterceptor<String,String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 每个消息（也叫记录record，我习惯叫消息）是由一个key，一个value和时间戳构成
     *
     * Producer在将消息序列化和分配分区之前会调用拦截器的这个方法来对消息进行相应的操作。
     * 一般来说最好不要修改消息ProducerRecord的topic、key以及partition等信息，如果要修改，
     * 也需确保对其有准确的判断，否则会与预想的效果出现偏差。比如修改key不仅会影响分区的计算，
     * 同样也会影响Broker端日志压缩（Log Compaction）的功能。
     * @param producerRecord
     * @return
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        if (producerRecord.value().length() <= 0) {
            return null;
        }

        return producerRecord;
    }

    /**
     * onAcknowledgement:承认，应答 RecordMetadata ： 记录元数据
     *
     * 在消息被应答（Acknowledgement）之前或者消息发送失败时调用，优先于用户设定的Callback之前执行。
     * 这个方法运行在Producer的IO线程中，所以这个方法里实现的代码逻辑越简单越好，否则会影响消息的发送速率。
     * @param recordMetadata
     * @param e
     */
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    /**
     * 关闭当前的拦截器，此方法主要用于执行一些资源的清理工作。
     */
    public void close() {
        double successRatio = (double)sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率="+String.format("%f", successRatio * 100)+"%");
    }

    /**
     * 用来初始化此类的方法，这个是ProducerInterceptor接口的父接口Configurable中的方法。
     * @param map
     */
    public void configure(Map<String, ?> map) {

    }
}
