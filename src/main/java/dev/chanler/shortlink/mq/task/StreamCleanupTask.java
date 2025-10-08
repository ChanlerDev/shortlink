package dev.chanler.shortlink.mq.task;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

import java.util.List;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;

/**
 * Stream 消息清理任务
 * 策略：保留 Pending 消息 + 最近已消费的缓冲，删除其他旧消息
 * 
 * 1. 获取最旧的 Pending 消息
 * 2. 使用 XTRIM MINID 删除该 ID 之前的消息
 * 3. 如果没有 Pending，则保留 last-delivered-id 之后的所有消息
 * 
 * @author: Chanler
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamCleanupTask {

    private final StringRedisTemplate stringRedisTemplate;
    
    /**
     * 已消费消息的保留缓冲数量（保留 last-delivered-id 之前）
     */
    private static final long CONSUMED_BUFFER_SIZE = 3000;

    
    /**
     * 每 5 分钟执行一次清理
     */
    @Scheduled(fixedRate = 300_000)
    public void cleanupConsumedMessages() {
        try {
            // 1. 获取最旧的 Pending 消息 ID（如果有 Pending 消息）
            String oldestPendingId = stringRedisTemplate.execute((RedisCallback<String>) connection -> {
                // XPENDING key group - + 1
                Object result = connection.execute("XPENDING",
                    SHORT_LINK_STATS_STREAM_TOPIC_KEY.getBytes(),
                    SHORT_LINK_STATS_STREAM_GROUP_KEY.getBytes(),
                    "-".getBytes(),
                    "+".getBytes(),
                    "1".getBytes()); // 只获取最旧的一条
                
                if (result instanceof List) {
                    List<?> pending = (List<?>) result;
                    if (!pending.isEmpty() && pending.get(0) instanceof List) {
                        // Pending 返回格式: [[id, consumer, idle, deliveryCount], ...]
                        Object firstEntry = ((List<?>) pending.get(0)).get(0);
                        return String.valueOf(firstEntry);
                    }
                }
                return null;
            });

            String trimToId;
            
            if (oldestPendingId != null) {
                // 有 Pending 消息：删除最旧 Pending 之前的消息
                trimToId = oldestPendingId;
                log.debug("Stream 清理: 检测到 Pending 消息，保留边界={}", trimToId);
            } else {
                // 没有 Pending 消息：获取 last-delivered-id，删除其之前的消息（保留缓冲）
                String lastDeliveredId = stringRedisTemplate.execute((RedisCallback<String>) connection -> {
                    Object result = connection.execute("XINFO",
                        "GROUPS".getBytes(),
                        SHORT_LINK_STATS_STREAM_TOPIC_KEY.getBytes());
                    
                    if (result instanceof List) {
                        List<?> groups = (List<?>) result;
                        for (int i = 0; i < groups.size(); i++) {
                            if ("last-delivered-id".equals(String.valueOf(groups.get(i))) && i + 1 < groups.size()) {
                                return String.valueOf(groups.get(i + 1));
                            }
                        }
                    }
                    return null;
                });

                if (lastDeliveredId == null || "0-0".equals(lastDeliveredId)) {
                    log.debug("Stream 清理: 无消费记录，跳过清理");
                    return;
                }

                // 保留 CONSUMED_BUFFER_SIZE 条缓冲：从 last-delivered-id 往前数 CONSUMED_BUFFER_SIZE 条
                trimToId = stringRedisTemplate.execute((RedisCallback<String>) connection -> {
                    Object result = connection.execute("XREVRANGE",
                        SHORT_LINK_STATS_STREAM_TOPIC_KEY.getBytes(),
                        lastDeliveredId.getBytes(),
                        "-".getBytes(),
                        "COUNT".getBytes(),
                        String.valueOf(CONSUMED_BUFFER_SIZE).getBytes());
                    
                    if (result instanceof List) {
                        List<?> messages = (List<?>) result;
                        if (!messages.isEmpty()) {
                            Object lastMsg = messages.get(messages.size() - 1);
                            if (lastMsg instanceof List) {
                                return String.valueOf(((List<?>) lastMsg).get(0));
                            }
                        }
                    }
                    return null; // 如果没有足够的历史消息，则不清理
                });
                
                if (trimToId == null) {
                    log.debug("Stream 清理: 无法确定清理边界");
                    return;
                }
            }

            // 2. 执行清理：删除 trimToId 之前的消息
            Long trimmed = stringRedisTemplate.execute((RedisCallback<Long>) connection -> {
                Object result = connection.execute("XTRIM",
                    SHORT_LINK_STATS_STREAM_TOPIC_KEY.getBytes(),
                    "MINID".getBytes(),
                    trimToId.getBytes());
                return result instanceof Long ? (Long) result : null;
            });

            if (trimmed != null && trimmed > 0) {
                log.info("Stream 清理成功: 清理边界={}, 删除了 {} 条消息", trimToId, trimmed);
            }

        } catch (Exception e) {
            log.error("Stream 清理失败", e);
        }
    }
}
