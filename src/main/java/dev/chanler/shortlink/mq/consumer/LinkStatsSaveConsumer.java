package dev.chanler.shortlink.mq.consumer;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.chanler.shortlink.common.convention.exception.ServiceException;
import dev.chanler.shortlink.dao.entity.*;
import dev.chanler.shortlink.dao.mapper.*;
import dev.chanler.shortlink.dto.biz.LinkStatsRecordDTO;
import dev.chanler.shortlink.mq.idempotent.MessageQueueIdempotentHandler;
import dev.chanler.shortlink.toolkit.ipgeo.GeoInfo;
import dev.chanler.shortlink.toolkit.ipgeo.IpGeoClient;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.*;

/**
 * 短链接监控状态保存消息队列消费者
 * @author: Chanler
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LinkStatsSaveConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private final LinkMapper linkMapper;
    private final LinkGotoMapper linkGotoMapper;
    private final RedissonClient redissonClient;
    private final IpGeoClient ipGeoClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final TransactionTemplate transactionTemplate;
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;

    private DefaultRedisScript<Long> hllCountAddDeltaScript;

    // 本地缓存：fullShortUrl -> gid
    private Cache<String, String> gidCache;

    // DB写入专用线程池（IO密集型）
    private ExecutorService dbWriteExecutor;

    private static final String HLL_COUNT_ADD_DELTA_LUA = "lua/hll_count_add_delta.lua";

    @PostConstruct
    public void init() {
        // 初始化 Lua 脚本
        hllCountAddDeltaScript = new DefaultRedisScript<>();
        hllCountAddDeltaScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(HLL_COUNT_ADD_DELTA_LUA)));
        hllCountAddDeltaScript.setResultType(Long.class);

        // 初始化本地缓存：最多缓存1万条，10分钟过期
        gidCache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build();

        // 初始化DB写入专用线程池（IO密集型任务）
        int coreThreads = 16;
        int maxThreads = 32;
        dbWriteExecutor = new ThreadPoolExecutor(
                coreThreads,
                maxThreads,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(200),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("db-writer-" + thread.getId());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        log.info("LinkStatsSaveConsumer initialized: gidCache={}, dbWriteExecutor core={}/max={}",
                gidCache.stats(), coreThreads, maxThreads);
    }

    /**
     * 失效 gid 缓存（当 gid 发生变更时调用）
     */
    public void invalidateGidCache(String fullShortUrl) {
        gidCache.invalidate(fullShortUrl);
        log.info("Invalidated gidCache for fullShortUrl={}", fullShortUrl);
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        RecordId id = message.getId();

        // 幂等检查
        if (messageQueueIdempotentHandler.isProcessed(id.toString())) {
            // 消息已被处理过（重复消费）
            if (messageQueueIdempotentHandler.isAccomplish(id.toString())) {
                // 已完成，补偿 ACK
                try {
                    stringRedisTemplate.opsForStream().acknowledge(
                        SHORT_LINK_STATS_STREAM_TOPIC_KEY,
                        SHORT_LINK_STATS_STREAM_GROUP_KEY,
                        id
                    );
                } catch (Exception e) {
                    log.warn("补偿 ACK 失败: {}", id, e);
                }
                return;
            }
            // 正在处理中，抛异常让消息留在 Pending
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        }

        try {
            // 业务逻辑
            Map<String, String> producerMap = message.getValue();
            LinkStatsRecordDTO statsRecord = JSON.parseObject(producerMap.get("statsRecord"), LinkStatsRecordDTO.class);
            actualSaveShortLinkStats(statsRecord);

        } catch (Throwable ex) {
            // 业务失败，删除幂等标记，不 ACK
            messageQueueIdempotentHandler.release(id.toString());
            log.error("业务逻辑执行失败，消息将重试: {}", id, ex);
            throw ex;
        }

        // 业务成功后，标记完成并 ACK
        try {
            messageQueueIdempotentHandler.setAccomplish(id.toString());
        } catch (Exception e) {
            log.error("设置幂等标记失败，但业务已成功，继续 ACK: {}", id, e);
        }

        try {
            stringRedisTemplate.opsForStream().acknowledge(
                SHORT_LINK_STATS_STREAM_TOPIC_KEY,
                SHORT_LINK_STATS_STREAM_GROUP_KEY,
                id
            );
        } catch (Exception e) {
            log.error("ACK 失败，但业务已成功且已标记，PEL 巡检会补偿: {}", id, e);
        }
    }

    public void actualSaveShortLinkStats(LinkStatsRecordDTO statsRecord) {
        String fullShortUrl = statsRecord.getFullShortUrl();

        // 阶段1：所有不涉及 gid 的准备工作

        // 计算时间相关字段
        Date eventTime = statsRecord.getCurrentDate();
        if (eventTime == null) {
            eventTime = new Date();
        }
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(eventTime.toInstant(), zoneId);
        int hour = zonedDateTime.getHour();
        int weekValue = zonedDateTime.getDayOfWeek().getValue();
        LocalDate localDate = zonedDateTime.toLocalDate();
        Date statsDate = Date.from(localDate.atStartOfDay(zoneId).toInstant());

        // 计算 v = epochDay(Asia/Shanghai) % 2（基于事件时间）
        int v = (int)(localDate.toEpochDay() % 2);

        // 使用新的 {v} 风格键
        String uvKey = String.format(STATS_UV_HLL_KEY, v, fullShortUrl);
        String uipKey = String.format(STATS_UIP_HLL_KEY, v, fullShortUrl);
        String uvActiveKey = String.format(STATS_UV_ACTIVE_KEY, v);
        String uipActiveKey = String.format(STATS_UIP_ACTIVE_KEY, v);

        // TTL 24小时（24 * 3600 = 86400秒）
        int ttlSeconds = 86400;

        // 计算 UV delta
        Long uvDelta = stringRedisTemplate.execute(hllCountAddDeltaScript,
            Arrays.asList(uvKey, uvActiveKey),
            statsRecord.getUv(),
            fullShortUrl,
            String.valueOf(ttlSeconds));

        // 计算 UIP delta
        Long uipDelta = stringRedisTemplate.execute(hllCountAddDeltaScript,
            Arrays.asList(uipKey, uipActiveKey),
            statsRecord.getUip(),
            fullShortUrl,
            String.valueOf(ttlSeconds));

        // 查询 IP 地理位置
        GeoInfo geoInfo = ipGeoClient.query(statsRecord.getUip());

        // 构建各维度统计实体
        LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                .fullShortUrl(fullShortUrl)
                .date(statsDate)
                .cnt(1)
                .province(geoInfo.getProvince())
                .city(geoInfo.getCity())
                .adcode(geoInfo.getAdcode())
                .country(geoInfo.getCountry())
                .build();

        LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                .os(statsRecord.getOs())
                .cnt(1)
                .fullShortUrl(fullShortUrl)
                .date(statsDate)
                .build();

        LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                .browser(statsRecord.getBrowser())
                .cnt(1)
                .fullShortUrl(fullShortUrl)
                .date(statsDate)
                .build();

        LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                .device(statsRecord.getDevice())
                .cnt(1)
                .fullShortUrl(fullShortUrl)
                .date(statsDate)
                .build();

        LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                .network(geoInfo.getIsp())
                .cnt(1)
                .fullShortUrl(fullShortUrl)
                .date(statsDate)
                .build();

        LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                .pv(1)
                .uv(uvDelta != null ? uvDelta.intValue() : 0)
                .uip(uipDelta != null ? uipDelta.intValue() : 0)
                .hour(hour)
                .weekday(weekValue)
                .fullShortUrl(fullShortUrl)
                .date(statsDate)
                .build();

        // 阶段2：6个统计维度并发写入
        try {
            CompletableFuture<Void> statsFuture = CompletableFuture.allOf(
                    CompletableFuture.runAsync(() ->
                            linkLocaleStatsMapper.shortLinkLocaleStats(linkLocaleStatsDO), dbWriteExecutor),
                    CompletableFuture.runAsync(() ->
                            linkOsStatsMapper.shortLinkOsStats(linkOsStatsDO), dbWriteExecutor),
                    CompletableFuture.runAsync(() ->
                            linkBrowserStatsMapper.shortLinkBrowserStats(linkBrowserStatsDO), dbWriteExecutor),
                    CompletableFuture.runAsync(() ->
                            linkDeviceStatsMapper.shortLinkDeviceStats(linkDeviceStatsDO), dbWriteExecutor),
                    CompletableFuture.runAsync(() ->
                            linkNetworkStatsMapper.shortLinkNetworkStats(linkNetworkStatsDO), dbWriteExecutor),
                    CompletableFuture.runAsync(() ->
                            linkAccessStatsMapper.shortLinkAccessStats(linkAccessStatsDO), dbWriteExecutor)
            );

            // access_logs 异步写入
            CompletableFuture<Void> logFuture = CompletableFuture.runAsync(() ->
                    saveAccessLogWithFirstFlag(fullShortUrl, statsRecord, geoInfo), dbWriteExecutor);

            // 阶段3：加读锁，获取 gid 并更新 link 表（这两个操作必须原子）
            RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(
                String.format(LOCK_GID_UPDATE_KEY, fullShortUrl)
            );
            RLock rLock = readWriteLock.readLock();
            rLock.lock();
            try {
                // 尝试从缓存获取 gid
                String gid = gidCache.getIfPresent(fullShortUrl);

                // 缓存未命中，查询数据库
                if (gid == null) {
                    LambdaQueryWrapper<LinkGotoDO> queryWrapper = Wrappers.lambdaQuery(LinkGotoDO.class)
                            .eq(LinkGotoDO::getFullShortUrl, fullShortUrl);
                    LinkGotoDO shortLinkGotoDO = linkGotoMapper.selectOne(queryWrapper);
                    if (shortLinkGotoDO == null) {
                        log.warn("LinkGotoDO not found for fullShortUrl={}, skip link stats update", fullShortUrl);
                        // 链接不存在，仍需等待异步任务完成
                    } else {
                        gid = shortLinkGotoDO.getGid();
                        // 写入缓存
                        gidCache.put(fullShortUrl, gid);
                    }
                }

                // gid 存在时才更新 link 表
                if (gid != null) {
                    // 使用获取到的 gid 更新 link 表
                    int affected = linkMapper.incrementStats(gid, fullShortUrl, 1,
                            uvDelta != null ? uvDelta.intValue() : 0,
                            uipDelta != null ? uipDelta.intValue() : 0);

                    // 检测更新失败（可能是 gid 已变更，或记录被删除）
                    if (affected == 0) {
                        log.warn("incrementStats affected 0 rows, gid may have changed or link deleted. fullShortUrl={}, oldGid={}",
                            fullShortUrl, gid);

                        // 失效缓存并重新查询
                        gidCache.invalidate(fullShortUrl);
                        LambdaQueryWrapper<LinkGotoDO> queryWrapper = Wrappers.lambdaQuery(LinkGotoDO.class)
                                .eq(LinkGotoDO::getFullShortUrl, fullShortUrl);
                        LinkGotoDO shortLinkGotoDO = linkGotoMapper.selectOne(queryWrapper);

                        if (shortLinkGotoDO != null && !shortLinkGotoDO.getGid().equals(gid)) {
                            // gid 确实变了，用新 gid 重试
                            String newGid = shortLinkGotoDO.getGid();
                            log.info("Detected gid change: {} -> {}, retrying incrementStats", gid, newGid);
                            affected = linkMapper.incrementStats(newGid, fullShortUrl, 1,
                                    uvDelta != null ? uvDelta.intValue() : 0,
                                    uipDelta != null ? uipDelta.intValue() : 0);

                            if (affected == 0) {
                                log.error("Retry incrementStats still failed after gid change, link may be deleted: {}", fullShortUrl);
                            } else {
                                // 重试成功，更新缓存
                                gidCache.put(fullShortUrl, newGid);
                            }
                        } else if (shortLinkGotoDO == null) {
                            log.error("Link not found in LinkGotoDO after incrementStats failed: {}", fullShortUrl);
                        } else {
                            // gid 没变但 affected=0，说明记录被删除了
                            log.warn("incrementStats affected 0 but gid unchanged, link has been deleted: {}", fullShortUrl);
                        }
                    }
                }
            } finally {
                rLock.unlock();
            }

            // 等待所有异步操作完成，超时1000ms
            CompletableFuture.allOf(statsFuture, logFuture).get(1000, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            log.error("异步写入统计数据失败，fullShortUrl={}", fullShortUrl, e);
            throw new ServiceException("统计数据写入失败: " + e.getMessage());
        }
    }

    private void saveAccessLogWithFirstFlag(String fullShortUrl,
                                            LinkStatsRecordDTO statsRecord,
                                            GeoInfo geoInfo) {
        transactionTemplate.executeWithoutResult(status -> {
            boolean isFirstVisit = false;
            String uv = statsRecord.getUv();
            if (StrUtil.isNotBlank(uv)) {
                LinkAccessLogsDO existed = linkAccessLogsMapper.selectOne(
                        Wrappers.lambdaQuery(LinkAccessLogsDO.class)
                                .eq(LinkAccessLogsDO::getFullShortUrl, fullShortUrl)
                                .eq(LinkAccessLogsDO::getUser, uv)
                                .eq(LinkAccessLogsDO::getDelFlag, 0)
                                .last("LIMIT 1 FOR UPDATE"));
                isFirstVisit = Objects.isNull(existed);
            }

            String locale = null;
            if (geoInfo != null) {
                locale = Stream.of(geoInfo.getCountry(), geoInfo.getProvince(), geoInfo.getCity())
                        .filter(StrUtil::isNotBlank)
                        .collect(Collectors.joining("-"));
                if (StrUtil.isBlank(locale)) {
                    locale = null;
                }
            }

            LinkAccessLogsDO accessLogsDO = LinkAccessLogsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .user(statsRecord.getUv())
                    .ip(statsRecord.getUip())
                    .browser(statsRecord.getBrowser())
                    .os(statsRecord.getOs())
                    .network(geoInfo != null ? geoInfo.getIsp() : null)
                    .device(statsRecord.getDevice())
                    .locale(locale)
                    .firstFlag(isFirstVisit)
                    .build();
            linkAccessLogsMapper.insert(accessLogsDO);
        });
    }
}
