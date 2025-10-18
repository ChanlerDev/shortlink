package dev.chanler.shortlink.common.config;

import dev.chanler.shortlink.mq.consumer.LinkStatsSaveConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * Redis Stream 消息队列配置
 * @author: Chanler
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfiguration {

    private final RedisConnectionFactory redisConnectionFactory;
    private final LinkStatsSaveConsumer linkStatsSaveConsumer;

    private static final long THROUGHPUT_LOG_INTERVAL_MS = 300_000L;
    private final LongAdder consumeCounter = new LongAdder();
    private final AtomicLong lastThroughputLogTime = new AtomicLong(System.currentTimeMillis());

    @Bean
    public ExecutorService asyncStreamConsumer() {
        // 并发线程数按 CPU 动态设置，至少 3，最多 4
        int nThreads = Math.min(4, Math.max(3, Runtime.getRuntime().availableProcessors() * 2));
        log.info("消费者线程池配置，线程数： {}", nThreads);
        AtomicInteger index = new AtomicInteger();
        return new ThreadPoolExecutor(
                nThreads,
                nThreads,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(600), // 改用有界队列，容量 600
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("stream_consumer_short-link_stats_" + index.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // 队列满时背压
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
        // 多线程消费者 + LinkedBlockingQueue + 批量拉取
        StreamMessageListenerContainer.StreamReadRequest<String> streamReadRequest =
                StreamMessageListenerContainer.StreamReadRequest.builder(
                                StreamOffset.create(SHORT_LINK_STATS_STREAM_TOPIC_KEY, ReadOffset.lastConsumed()))
                        .cancelOnError(throwable -> false)
                        .consumer(Consumer.from(SHORT_LINK_STATS_STREAM_GROUP_KEY, "stats-consumer"))
                        .autoAcknowledge(false)
                        .build();

        StreamListener<String, MapRecord<String, String, String>> loggingListener = message -> {
            consumeCounter.increment();
            linkStatsSaveConsumer.onMessage(message);

            long now = System.currentTimeMillis();
            long last = lastThroughputLogTime.get();
            if (now - last >= THROUGHPUT_LOG_INTERVAL_MS && lastThroughputLogTime.compareAndSet(last, now)) {
                long processed = consumeCounter.sumThenReset();
                if (processed > 0) {
                    long tps = processed * 1000 / THROUGHPUT_LOG_INTERVAL_MS;
                    log.info("Stream 消费的吞吐量: {} msgs / {} ms (≈{} TPS)",
                            processed, THROUGHPUT_LOG_INTERVAL_MS, tps);
                }
            }
        };

        return streamMessageListenerContainer.register(streamReadRequest, loggingListener);
    }
}
