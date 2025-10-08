package dev.chanler.shortlink.common.config;

import dev.chanler.shortlink.mq.consumer.LinkStatsSaveConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * Redis Stream 消息队列配置
 * @author: Chanler
 */
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfiguration {

    private final RedisConnectionFactory redisConnectionFactory;
    private final LinkStatsSaveConsumer linkStatsSaveConsumer;

    @Bean
    public ExecutorService asyncStreamConsumer() {
        // 并发线程数按 CPU 动态设置，至少 2，最多 8
        int nThreads = Math.min(4, Math.max(2, Runtime.getRuntime().availableProcessors()));
        AtomicInteger index = new AtomicInteger();
        return new ThreadPoolExecutor(nThreads,
                nThreads,
                60,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("stream_consumer_short-link_stats_" + index.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(
            ExecutorService asyncStreamConsumer) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .batchSize(200) // 提升批量抓取，提高吞吐
                        .executor(asyncStreamConsumer)
                        .pollTimeout(Duration.ofMillis(500)) // 更快的轮询以降低延迟
                        .build();
        return StreamMessageListenerContainer.create(redisConnectionFactory, options);
    }

    @Bean(destroyMethod = "cancel")
    public Subscription shortLinkStatsSaveConsumerSubscription(
            StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer) {
        // 单消费者 + SynchronousQueue + CallerRunsPolicy = 天然背压
        StreamMessageListenerContainer.StreamReadRequest<String> streamReadRequest =
                StreamMessageListenerContainer.StreamReadRequest.builder(
                                StreamOffset.create(SHORT_LINK_STATS_STREAM_TOPIC_KEY, ReadOffset.lastConsumed()))
                        .cancelOnError(throwable -> false)
                        .consumer(Consumer.from(SHORT_LINK_STATS_STREAM_GROUP_KEY, "stats-consumer"))
                        .autoAcknowledge(false)
                        .build();
        return streamMessageListenerContainer.register(streamReadRequest, linkStatsSaveConsumer);
    }
}
