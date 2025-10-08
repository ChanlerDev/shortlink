package dev.chanler.shortlink.mq.task;

import dev.chanler.shortlink.mq.consumer.LinkStatsSaveConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.domain.Range;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * Pending 消息恢复任务（单线程定时巡检）
 * @author: Chanler
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PendingMessageRecoveryTask {

    private final StringRedisTemplate stringRedisTemplate;
    private final LinkStatsSaveConsumer consumer;

    @Scheduled(fixedRate = 30000) // 每30秒检查一次
    public void recoverPendingMessages() {
        try {
            // 1. 查询 Pending 总数
            PendingMessagesSummary summary = stringRedisTemplate.opsForStream()
                .pending(SHORT_LINK_STATS_STREAM_TOPIC_KEY,
                         SHORT_LINK_STATS_STREAM_GROUP_KEY);

            if (summary == null || summary.getTotalPendingMessages() == 0) {
                return;
            }

            // 2. 获取 Pending 列表（最多100条）
            PendingMessages messages = stringRedisTemplate.opsForStream()
                .pending(SHORT_LINK_STATS_STREAM_TOPIC_KEY,
                         SHORT_LINK_STATS_STREAM_GROUP_KEY,
                         Range.unbounded(),
                         100L);

            if (messages == null || messages.isEmpty()) {
                return;
            }

            // 3. 批量收集所有 Pending 的 ID
            RecordId[] allPendingIds = messages.stream()
                .map(PendingMessage::getId)
                .toArray(RecordId[]::new);

            // 4. XCLAIM 的 minIdleTime 会自动过滤：只认领 idle > 5分钟的消息
            List<MapRecord<String, Object, Object>> claimed =
                stringRedisTemplate.opsForStream().claim(
                    SHORT_LINK_STATS_STREAM_TOPIC_KEY,
                    SHORT_LINK_STATS_STREAM_GROUP_KEY,
                    "stats-consumer",
                    Duration.ofMinutes(5),  // 只认领 idle > 5分钟的
                    allPendingIds
                );

            // 5. 处理认领到的消息（已被 Redis 自动过滤）
            if (claimed == null || claimed.isEmpty()) {
                return;
            }

            int recoveredCount = 0;
            for (MapRecord<String, Object, Object> record : claimed) {
                try {
                    MapRecord<String, String, String> typedRecord =
                        (MapRecord<String, String, String>) (MapRecord<?, ?, ?>) record;
                    consumer.onMessage(typedRecord);
                    recoveredCount++;
                } catch (Exception e) {
                    log.error("恢复 Pending 消息失败: {}", record.getId(), e);
                }
            }

            log.info("PEL 巡检: 发现 {} 条 Pending，恢复 {} 条超时消息",
                     messages.size(), recoveredCount);

        } catch (Exception e) {
            log.error("PEL 恢复任务执行失败", e);
        }
    }
}
