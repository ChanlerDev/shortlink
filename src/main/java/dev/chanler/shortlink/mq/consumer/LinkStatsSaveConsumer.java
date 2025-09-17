package dev.chanler.shortlink.mq.consumer;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
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

    private static final String HLL_COUNT_ADD_DELTA_LUA = "lua/hll_count_add_delta.lua";

    @PostConstruct
    public void init() {
        hllCountAddDeltaScript = new DefaultRedisScript<>();
        hllCountAddDeltaScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(HLL_COUNT_ADD_DELTA_LUA)));
        hllCountAddDeltaScript.setResultType(Long.class);
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String stream = message.getStream();
        RecordId id = message.getId();
        if (messageQueueIdempotentHandler.isMessageBeingConsumed(id.toString())) {
            // 判断当前的这个消息流程是否执行完成
            if (messageQueueIdempotentHandler.isAccomplish(id.toString())) {
                return;
            }
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        }
        try {
            Map<String, String> producerMap = message.getValue();
            LinkStatsRecordDTO statsRecord = JSON.parseObject(producerMap.get("statsRecord"), LinkStatsRecordDTO.class);
            actualSaveShortLinkStats(statsRecord);
            stringRedisTemplate.opsForStream().delete(Objects.requireNonNull(stream), id.getValue());
        } catch (Throwable ex) {
            // 某某某情况宕机了
            messageQueueIdempotentHandler.delMessageProcessed(id.toString());
            log.error("记录短链接监控消费异常", ex);
            throw ex;
        }
        messageQueueIdempotentHandler.setAccomplish(id.toString());
    }

    public void actualSaveShortLinkStats(LinkStatsRecordDTO statsRecord) {
        String fullShortUrl = statsRecord.getFullShortUrl();
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, fullShortUrl));
        RLock rLock = readWriteLock.readLock();
        rLock.lock();
        try {
            LambdaQueryWrapper<LinkGotoDO> queryWrapper = Wrappers.lambdaQuery(LinkGotoDO.class)
                    .eq(LinkGotoDO::getFullShortUrl, fullShortUrl);
            LinkGotoDO shortLinkGotoDO = linkGotoMapper.selectOne(queryWrapper);
            if (shortLinkGotoDO == null) {
                log.warn("LinkGotoDO not found for fullShortUrl={}, skip stats persist", fullShortUrl);
                return;
            }
            String gid = shortLinkGotoDO.getGid();
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

            GeoInfo geoInfo = ipGeoClient.query(statsRecord.getUip());
            LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .date(statsDate)
                    .cnt(1)
                    .province(geoInfo.getProvince())
                    .city(geoInfo.getCity())
                    .adcode(geoInfo.getAdcode())
                    .country(geoInfo.getCountry())
                    .build();
            linkLocaleStatsMapper.shortLinkLocaleStats(linkLocaleStatsDO);
            LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                    .os(statsRecord.getOs())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(statsDate)
                    .build();
            linkOsStatsMapper.shortLinkOsStats(linkOsStatsDO);
            LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                    .browser(statsRecord.getBrowser())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(statsDate)
                    .build();
            linkBrowserStatsMapper.shortLinkBrowserStats(linkBrowserStatsDO);
            LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                    .device(statsRecord.getDevice())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(statsDate)
                    .build();
            linkDeviceStatsMapper.shortLinkDeviceStats(linkDeviceStatsDO);
            LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                    .network(geoInfo.getIsp())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(statsDate)
                    .build();
            linkNetworkStatsMapper.shortLinkNetworkStats(linkNetworkStatsDO);

            saveAccessLogWithFirstFlag(fullShortUrl, statsRecord, geoInfo);
            
            // 使用 delta 写入明细统计
            LinkAccessStatsDO linkAccessStatsDOWithDelta = LinkAccessStatsDO.builder()
                    .pv(1)
                    .uv(uvDelta != null ? uvDelta.intValue() : 0)
                    .uip(uipDelta != null ? uipDelta.intValue() : 0)
                    .hour(hour)
                    .weekday(weekValue)
                    .fullShortUrl(fullShortUrl)
                    .date(statsDate)
                    .build();
            linkAccessStatsMapper.shortLinkAccessStats(linkAccessStatsDOWithDelta);
            
            // 更新链接统计，使用计算出的 delta
            linkMapper.incrementStats(gid, fullShortUrl, 1, 
                uvDelta != null ? uvDelta.intValue() : 0, 
                uipDelta != null ? uipDelta.intValue() : 0);
        } finally {
            rLock.unlock();
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
