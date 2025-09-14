package dev.chanler.shortlink.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import dev.chanler.shortlink.common.biz.user.UserContext;
import dev.chanler.shortlink.common.config.GotoDomainWhiteListConfiguration;
import dev.chanler.shortlink.common.biz.user.GroupOwnershipVerifier;
import dev.chanler.shortlink.common.convention.exception.ClientException;
import dev.chanler.shortlink.common.convention.exception.ServiceException;
import dev.chanler.shortlink.common.enums.ValidDateTypeEnum;
import dev.chanler.shortlink.dao.entity.*;
import dev.chanler.shortlink.dao.mapper.*;
import dev.chanler.shortlink.dto.biz.LinkStatsRecordDTO;
import dev.chanler.shortlink.dto.req.LinkBatchCreateReqDTO;
import dev.chanler.shortlink.dto.req.LinkCreateReqDTO;
import dev.chanler.shortlink.dto.req.LinkPageReqDTO;
import dev.chanler.shortlink.dto.req.LinkUpdateReqDTO;
import dev.chanler.shortlink.dto.resp.*;
import dev.chanler.shortlink.mq.producer.LinkStatsSaveProducer;
import dev.chanler.shortlink.service.LinkService;
import dev.chanler.shortlink.toolkit.HashUtil;
import dev.chanler.shortlink.toolkit.LinkUtil;
import dev.chanler.shortlink.toolkit.ShortCodeUtil;
import dev.chanler.shortlink.toolkit.ipgeo.GeoInfo;
import dev.chanler.shortlink.toolkit.ipgeo.IpGeoClient;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static dev.chanler.shortlink.common.constant.RedisKeyConstant.*;
import static dev.chanler.shortlink.common.constant.UserConstant.PUBLIC_GID;
import static dev.chanler.shortlink.common.constant.UserConstant.PUBLIC_USERNAME;

/**
 * 短链接接口实现层
 * @author: Chanler
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LinkServiceImpl extends ServiceImpl<LinkMapper, LinkDO> implements LinkService {

    private final RBloomFilter<String> shortUriCreateCachePenetrationBloomFilter;
    private final LinkGotoMapper linkGotoMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final IpGeoClient ipGeoClient;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;
    private final GotoDomainWhiteListConfiguration gotoDomainWhiteListConfiguration;
    private final LinkStatsSaveProducer linkStatsSaveProducer;
    private final GroupOwnershipVerifier groupOwnershipService;

    @Value("${short-link.domain.default}")
    private String createLinkDefaultDomain;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public LinkCreateRespDTO createLink(LinkCreateReqDTO linkCreateReqDTO) {
        // 未登录（public）创建：强制使用公共分组
        String currentUsername = UserContext.getUsername();
        if (java.util.Objects.equals(currentUsername, PUBLIC_USERNAME)) {
            linkCreateReqDTO.setGid(PUBLIC_GID);
        } else {
            // 鉴权：校验分组归属
            groupOwnershipService.assertOwnedByCurrentUser(linkCreateReqDTO.getGid());
        }
        verificationWhitelist(linkCreateReqDTO.getOriginUrl());
        
        // 设置默认值
        if (linkCreateReqDTO.getCreatedType() == null) {
            linkCreateReqDTO.setCreatedType(0);
        }
        if (linkCreateReqDTO.getValidDateType() == null) {
            linkCreateReqDTO.setValidDateType(ValidDateTypeEnum.CUSTOM.getType());
        }
        
        // 处理有效期逻辑，限制最大3天
        Date now = new Date();
        Date maxValidDate = DateUtil.offsetDay(now, 3);
        
        if (linkCreateReqDTO.getValidDate() == null) {
            // 如果没有传入validDate，默认设置为1天后
            linkCreateReqDTO.setValidDate(DateUtil.offsetDay(now, 1));
        } else if (linkCreateReqDTO.getValidDate().after(maxValidDate)) {
            // 如果传入的validDate超过3天，修正为3天
            linkCreateReqDTO.setValidDate(maxValidDate);
        }
        
        // 确保不是永久有效
        if (linkCreateReqDTO.getValidDateType() == ValidDateTypeEnum.PERMANENT.getType()) {
            linkCreateReqDTO.setValidDateType(ValidDateTypeEnum.CUSTOM.getType());
        }
        
        String shortLinkSuffix = generateSuffix(linkCreateReqDTO);
        String fullShortUrl = StrBuilder.create(createLinkDefaultDomain)
                .append("/")
                .append(shortLinkSuffix)
                .toString();
        LinkDO shortLinkDO = LinkDO.builder()
                .domain(createLinkDefaultDomain)
                .originUrl(linkCreateReqDTO.getOriginUrl())
                .gid(linkCreateReqDTO.getGid())
                .createdType(linkCreateReqDTO.getCreatedType())
                .validDateType(linkCreateReqDTO.getValidDateType())
                .validDate(linkCreateReqDTO.getValidDate())
                .describe(linkCreateReqDTO.getDescribe())
                .shortUri(shortLinkSuffix)
                .enableStatus(0)
                .totalPv(0)
                .totalUv(0)
                .totalUip(0)
                .delTime(0L)
                .fullShortUrl(fullShortUrl)
                .favicon(getFavicon(linkCreateReqDTO.getOriginUrl()))
                .build();
        LinkGotoDO linkGotoDO = LinkGotoDO.builder()
                .fullShortUrl(fullShortUrl)
                .gid(linkCreateReqDTO.getGid())
                .build();
        try {
            baseMapper.insert(shortLinkDO);
            linkGotoMapper.insert(linkGotoDO);
        } catch (DuplicateKeyException ex) {
            // 首先判断是否存在布隆过滤器，如果不存在直接新增
            if (!shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl)) {
                shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
            }
            throw new ServiceException(String.format("短链接：%s 生成重复", fullShortUrl));
        }
        // 缓存预热
        stringRedisTemplate.opsForValue().set(
                String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                linkCreateReqDTO.getOriginUrl(),
                LinkUtil.getLinkCacheValidTime(linkCreateReqDTO.getValidDate()), TimeUnit.MILLISECONDS
        );
        shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
        return LinkCreateRespDTO.builder()
                .fullShortUrl("http://" + shortLinkDO.getFullShortUrl())
                .originUrl(linkCreateReqDTO.getOriginUrl())
                .gid(linkCreateReqDTO.getGid())
                .build();
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void updateLink(LinkUpdateReqDTO linkUpdateReqDTO) {
        // 鉴权：旧、新分组均需属于当前用户
        groupOwnershipService.assertOwnedByCurrentUser(linkUpdateReqDTO.getOriginGid());
        groupOwnershipService.assertOwnedByCurrentUser(linkUpdateReqDTO.getGid());
        verificationWhitelist(linkUpdateReqDTO.getOriginUrl());
        LambdaQueryWrapper<LinkDO> queryWrapper = Wrappers.lambdaQuery(LinkDO.class)
                .eq(LinkDO::getGid, linkUpdateReqDTO.getOriginGid())
                .eq(LinkDO::getFullShortUrl, linkUpdateReqDTO.getFullShortUrl())
                .eq(LinkDO::getDelFlag, 0)
                .eq(LinkDO::getEnableStatus, 0);
        LinkDO hasLinkDO = baseMapper.selectOne(queryWrapper);
        if (hasLinkDO == null) {
            throw new ClientException("短链接记录不存在");
        }
        if (Objects.equals(hasLinkDO.getGid(), linkUpdateReqDTO.getGid())) {
            LambdaUpdateWrapper<LinkDO> updateWrapper = Wrappers.lambdaUpdate(LinkDO.class)
                    .eq(LinkDO::getFullShortUrl, linkUpdateReqDTO.getFullShortUrl())
                    .eq(LinkDO::getGid, linkUpdateReqDTO.getGid())
                    .eq(LinkDO::getDelFlag, 0)
                    .eq(LinkDO::getEnableStatus, 0)
                    .set(Objects.equals(linkUpdateReqDTO.getValidDateType(), ValidDateTypeEnum.PERMANENT.getType()), LinkDO::getValidDate, null);
            LinkDO linkDO = LinkDO.builder()
                    .domain(hasLinkDO.getDomain())
                    .shortUri(hasLinkDO.getShortUri())
                    .favicon(Objects.equals(linkUpdateReqDTO.getOriginUrl(), hasLinkDO.getOriginUrl()) ? hasLinkDO.getFavicon() : getFavicon(linkUpdateReqDTO.getOriginUrl()))
                    .createdType(hasLinkDO.getCreatedType())
                    .gid(linkUpdateReqDTO.getGid())
                    .originUrl(linkUpdateReqDTO.getOriginUrl())
                    .describe(linkUpdateReqDTO.getDescribe())
                    .validDateType(linkUpdateReqDTO.getValidDateType())
                    .validDate(linkUpdateReqDTO.getValidDate())
                    .build();
            baseMapper.update(linkDO, updateWrapper);
        } else {
            RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, linkUpdateReqDTO.getFullShortUrl()));
            RLock rLock = readWriteLock.writeLock();
            rLock.lock();
            try {
                LambdaUpdateWrapper<LinkDO> linkUpdateWrapper = Wrappers.lambdaUpdate(LinkDO.class)
                        .eq(LinkDO::getFullShortUrl, linkUpdateReqDTO.getFullShortUrl())
                        .eq(LinkDO::getGid, hasLinkDO.getGid())
                        .eq(LinkDO::getDelFlag, 0)
                        .eq(LinkDO::getDelTime, 0L)
                        .eq(LinkDO::getEnableStatus, 0);
                LinkDO delLinkDO = LinkDO.builder()
                        .delTime(System.currentTimeMillis())
                        .build();
                delLinkDO.setDelFlag(1);
                baseMapper.update(delLinkDO, linkUpdateWrapper);
                LinkDO linkDO = LinkDO.builder()
                        .domain(createLinkDefaultDomain)
                        .originUrl(linkUpdateReqDTO.getOriginUrl())
                        .gid(linkUpdateReqDTO.getGid())
                        .createdType(hasLinkDO.getCreatedType())
                        .validDateType(linkUpdateReqDTO.getValidDateType())
                        .validDate(linkUpdateReqDTO.getValidDate())
                        .describe(linkUpdateReqDTO.getDescribe())
                        .shortUri(hasLinkDO.getShortUri())
                        .enableStatus(hasLinkDO.getEnableStatus())
                        .totalPv(hasLinkDO.getTotalPv())
                        .totalUv(hasLinkDO.getTotalUv())
                        .totalUip(hasLinkDO.getTotalUip())
                        .fullShortUrl(hasLinkDO.getFullShortUrl())
                        .favicon(Objects.equals(linkUpdateReqDTO.getOriginUrl(), hasLinkDO.getOriginUrl()) ? hasLinkDO.getFavicon() : getFavicon(linkUpdateReqDTO.getOriginUrl()))
                        .delTime(0L)
                        .build();
                baseMapper.insert(linkDO);
                LambdaQueryWrapper<LinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(LinkGotoDO.class)
                        .eq(LinkGotoDO::getFullShortUrl, linkUpdateReqDTO.getFullShortUrl())
                        .eq(LinkGotoDO::getGid, hasLinkDO.getGid());
                LinkGotoDO linkGotoDO = linkGotoMapper.selectOne(linkGotoQueryWrapper);
                linkGotoMapper.delete(linkGotoQueryWrapper);
                linkGotoDO.setGid(linkUpdateReqDTO.getGid());
                linkGotoMapper.insert(linkGotoDO);
            } finally {
                rLock.unlock();
            }
        }
        if (!Objects.equals(hasLinkDO.getValidDateType(), linkUpdateReqDTO.getValidDateType())
                || !Objects.equals(hasLinkDO.getValidDate(), linkUpdateReqDTO.getValidDate())
                || !Objects.equals(hasLinkDO.getOriginUrl(), linkUpdateReqDTO.getOriginUrl())) {
            stringRedisTemplate.delete(String.format(GOTO_SHORT_LINK_KEY, linkUpdateReqDTO.getFullShortUrl()));
            Date currentDate = new Date();
            if (hasLinkDO.getValidDate() != null && hasLinkDO.getValidDate().before(currentDate)) {
                if (Objects.equals(linkUpdateReqDTO.getValidDateType(), ValidDateTypeEnum.PERMANENT.getType()) || linkUpdateReqDTO.getValidDate().after(currentDate)) {
                    stringRedisTemplate.delete(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, linkUpdateReqDTO.getFullShortUrl()));
                }
            }
        }
    }

    @Override
    public IPage<LinkPageRespDTO> pageLink(LinkPageReqDTO linkPageReqDTO) {
        // 鉴权：校验分组归属
        groupOwnershipService.assertOwnedByCurrentUser(linkPageReqDTO.getGid());
        IPage<LinkDO> resultPage = baseMapper.pageLink(linkPageReqDTO);
        return resultPage.convert(each -> {
            LinkPageRespDTO bean = BeanUtil.toBean(each, LinkPageRespDTO.class);
            bean.setDomain("http://" + bean.getDomain());
            return bean;
        });
    }

    @Override
    public List<GroupLinkCountQueryRespDTO> listGroupLinkCount(List<String> gidList) {
        // 鉴权：校验分组列表归属
        groupOwnershipService.assertAllOwnedByCurrentUser(gidList);
        QueryWrapper<LinkDO> queryWrapper = Wrappers.query(new LinkDO())
                .select("gid as gid, count(*) as shortLinkCount")
                .in("gid", gidList)
                .eq("enable_status", 0)
                .eq("del_flag", 0)
                .groupBy("gid");
        List<Map<String, Object>> shortLinkDOList = baseMapper.selectMaps(queryWrapper);
        return BeanUtil.copyToList(shortLinkDOList, GroupLinkCountQueryRespDTO.class);
    }

    @SneakyThrows
    @Override
    public void restoreUrl(String shortUri, ServletRequest request, ServletResponse response) {
        String serverName = request.getServerName();
        String serverPort = Optional.of(request.getServerPort())
                .filter(each -> !Objects.equals(each, 80))
                .map(String::valueOf)
                .map(each -> ":" + each)
                .orElse("");
        String fullShortUrl = StrBuilder.create(serverName)
                .append(serverPort)
                .append("/")
                .append(shortUri)
                .toString();
        String originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(originalLink)) {
            linkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
            ((HttpServletResponse) response).sendRedirect(originalLink);
            return;
        }
        boolean contains = ShortCodeUtil.mightExist(shortUri);
        if (!contains) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        contains = shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl);
        if (!contains) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        String gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        RLock lock = redissonClient.getLock(String.format(LOCK_GOTO_SHORT_LINK_KEY, fullShortUrl));
        lock.lock();
        try {
            originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(originalLink)) {
                linkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
                ((HttpServletResponse) response).sendRedirect(originalLink);
                return;
            }
            gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            LambdaQueryWrapper<LinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(LinkGotoDO.class)
                    .eq(LinkGotoDO::getFullShortUrl, fullShortUrl);
            LinkGotoDO linkGotoDO = linkGotoMapper.selectOne(linkGotoQueryWrapper);
            if (linkGotoDO == null) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            LambdaQueryWrapper<LinkDO> queryWrapper = Wrappers.lambdaQuery(LinkDO.class)
                    .eq(LinkDO::getGid, linkGotoDO.getGid())
                    .eq(LinkDO::getFullShortUrl, fullShortUrl)
                    .eq(LinkDO::getDelFlag, 0)
                    .eq(LinkDO::getEnableStatus, 0);
            LinkDO linkDO = baseMapper.selectOne(queryWrapper);
            if (linkDO == null || (linkDO.getValidDate() != null && linkDO.getValidDate().before(new Date()))) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            stringRedisTemplate.opsForValue().set(
                    String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                    linkDO.getOriginUrl(),
                    LinkUtil.getLinkCacheValidTime(linkDO.getValidDate()), TimeUnit.MILLISECONDS
            );
            linkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
            ((HttpServletResponse) response).sendRedirect(linkDO.getOriginUrl());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public LinkBatchCreateRespDTO batchCreateLink(LinkBatchCreateReqDTO linkBatchCreateReqDTO) {
        List<String> originUrls = linkBatchCreateReqDTO.getOriginUrls();
        List<String> describes = linkBatchCreateReqDTO.getDescribes();
        List<LinkBaseInfoRespDTO> result = new ArrayList<>();
        for (int i = 0; i < originUrls.size(); i++) {
            LinkCreateReqDTO shortLinkCreateReqDTO = BeanUtil.toBean(linkBatchCreateReqDTO, LinkCreateReqDTO.class);
            shortLinkCreateReqDTO.setOriginUrl(originUrls.get(i));
            shortLinkCreateReqDTO.setDescribe(describes.get(i));
            try {
                LinkCreateRespDTO shortLink = createLink(shortLinkCreateReqDTO);
                LinkBaseInfoRespDTO linkBaseInfoRespDTO = LinkBaseInfoRespDTO.builder()
                        .fullShortUrl(shortLink.getFullShortUrl())
                        .originUrl(shortLink.getOriginUrl())
                        .describe(describes.get(i))
                        .build();
                result.add(linkBaseInfoRespDTO);
            } catch (Throwable ex) {
                log.error("批量创建短链接失败，原始参数：{}", originUrls.get(i));
            }
        }
        return LinkBatchCreateRespDTO.builder()
                .total(result.size())
                .baseLinkInfos(result)
                .build();
    }

    private LinkStatsRecordDTO buildLinkStatsRecordAndSetUser(String fullShortUrl, ServletRequest request, ServletResponse response) {
        AtomicBoolean uvFirstFlag = new AtomicBoolean();
        Cookie[] cookies = ((HttpServletRequest) request).getCookies();
        AtomicReference<String> uv = new AtomicReference<>();
        Runnable addResponseCookieTask = () -> {
            uv.set(UUID.fastUUID().toString());
            Cookie uvCookie = new Cookie("uv", uv.get());
            uvCookie.setMaxAge(60 * 60 * 24 * 30);
            uvCookie.setPath(StrUtil.sub(fullShortUrl, fullShortUrl.indexOf("/"), fullShortUrl.length()));
            ((HttpServletResponse) response).addCookie(uvCookie);
            uvFirstFlag.set(Boolean.TRUE);
            stringRedisTemplate.opsForSet().add(String.format(SHORT_LINK_STATS_UV_KEY, fullShortUrl), uv.get());
        };
        if (ArrayUtil.isNotEmpty(cookies)) {
            Arrays.stream(cookies)
                    .filter(each -> Objects.equals(each.getName(), "uv"))
                    .findFirst()
                    .map(Cookie::getValue)
                    .ifPresentOrElse(each -> {
                        uv.set(each);
                        Long uvAdded = stringRedisTemplate.opsForSet().add(String.format(SHORT_LINK_STATS_UV_KEY, fullShortUrl), each);
                        uvFirstFlag.set(uvAdded != null && uvAdded > 0L);
                    }, addResponseCookieTask);
        } else {
            addResponseCookieTask.run();
        }
        String uip = LinkUtil.getActualIp(((HttpServletRequest) request));
        String os = LinkUtil.getOs(((HttpServletRequest) request));
        String browser = LinkUtil.getBrowser(((HttpServletRequest) request));
        String device = LinkUtil.getDevice(((HttpServletRequest) request));
        Long uipAdded = stringRedisTemplate.opsForSet().add(String.format(SHORT_LINK_STATS_UIP_KEY, fullShortUrl), uip);
        boolean uipFirstFlag = uipAdded != null && uipAdded > 0L;
        return LinkStatsRecordDTO.builder()
                .fullShortUrl(fullShortUrl)
                .uv(uv.get())
                .uvFirstFlag(uvFirstFlag.get())
                .uipFirstFlag(uipFirstFlag)
                .uip(uip)
                .os(os)
                .browser(browser)
                .device(device)
                .currentDate(new Date())
                .build();
    }

    @Override
    public void linkStats(LinkStatsRecordDTO linkStatsRecordDTO) {
        Map<String, String> producerMap = new HashMap<>();
        producerMap.put("statsRecord", JSON.toJSONString(linkStatsRecordDTO));
        linkStatsSaveProducer.send(producerMap);
    }

    private void oldLinkStats(String fullShortUrl, ServletRequest request, ServletResponse response) {
        AtomicBoolean uvFirstFlag = new AtomicBoolean();
        Cookie[] cookies = ((HttpServletRequest) request).getCookies();
        try {
            AtomicReference<String> uv = new AtomicReference<>();
            Runnable addResponseCookieTask = () -> {
                uv.set(UUID.fastUUID().toString());
                Cookie uvCookie = new Cookie("uv", uv.get());
                uvCookie.setMaxAge(60 * 60 * 24 * 30);
                uvCookie.setPath(StrUtil.sub(fullShortUrl, fullShortUrl.indexOf("/"), fullShortUrl.length()));
                ((HttpServletResponse) response).addCookie(uvCookie);
                uvFirstFlag.set(Boolean.TRUE);
                stringRedisTemplate.opsForSet().add("short-link:stats:uv:" + fullShortUrl, uv.get());
            };
            if (ArrayUtil.isNotEmpty(cookies)) {
                Arrays.stream(cookies)
                        .filter(cookie -> Objects.equals(cookie.getName(), "uv"))
                        .findFirst()
                        .map(Cookie::getValue)
                        .ifPresentOrElse(cookie -> {
                            uv.set(cookie);
                            Long uvAdded = stringRedisTemplate.opsForSet().add("short-link:stats:uv:" + fullShortUrl, cookie);
                            uvFirstFlag.set(uvAdded != null && uvAdded > 0L);
                        }, addResponseCookieTask);
            } else {
                addResponseCookieTask.run();
            }
            String uip = LinkUtil.getActualIp(((HttpServletRequest) request));
            Long uipAdded = stringRedisTemplate.opsForSet().add("short-link:stats:uip:" + fullShortUrl, uip);
            Boolean uipFirstFlag = uipAdded != null && uipAdded > 0L;
            int hour = DateUtil.hour(new Date(), true);
            int weekDay = DateUtil.dayOfWeekEnum(new Date()).getIso8601Value();
            LinkAccessStatsDO linkAccessStats = LinkAccessStatsDO.builder()
                    .pv(1)
                    .uv(uvFirstFlag.get() ? 1 : 0)
                    .uip(uipFirstFlag ? 1 : 0)
                    .hour(hour)
                    .weekday(weekDay)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkAccessStatsMapper.shortLinkAccessStats(linkAccessStats);
            GeoInfo geoInfo = ipGeoClient.query(uip);
            LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .cnt(1)
                    .province(geoInfo.getProvince())
                    .city(geoInfo.getCity())
                    .adcode(geoInfo.getAdcode())
                    .country(geoInfo.getCountry())
                    .build();
            linkLocaleStatsMapper.shortLinkLocaleStats(linkLocaleStatsDO);

            String os = LinkUtil.getOs((HttpServletRequest) request);
            LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .cnt(1)
                    .os(os)
                    .build();
            linkOsStatsMapper.shortLinkOsStats(linkOsStatsDO);

            String browser = LinkUtil.getBrowser((HttpServletRequest) request);
            LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .cnt(1)
                    .browser(browser)
                    .build();
            linkBrowserStatsMapper.shortLinkBrowserStats(linkBrowserStatsDO);

            String device = LinkUtil.getDevice((HttpServletRequest) request);
            LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .cnt(1)
                    .device(device)
                    .build();
            linkDeviceStatsMapper.shortLinkDeviceStats(linkDeviceStatsDO);

            String network = LinkUtil.getNetwork(geoInfo);
            LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .cnt(1)
                    .network(network)
                    .build();
            linkNetworkStatsMapper.shortLinkNetworkStats(linkNetworkStatsDO);

            LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .user(uv.get())
                    .browser(browser)
                    .os(os)
                    .ip(uip)
                    .network(network)
                    .device(device)
                    .locale(StrBuilder.create(geoInfo.getCountry())
                            .append("-")
                            .append(geoInfo.getProvince())
                            .append("-")
                            .append(geoInfo.getCity())
                            .toString())
                    .build();
            linkAccessLogsMapper.insert(linkAccessLogsDO);

            String gid = linkGotoMapper.selectOne(Wrappers.lambdaQuery(LinkGotoDO.class)
                            .select(LinkGotoDO::getGid)
                            .eq(LinkGotoDO::getFullShortUrl, fullShortUrl)).getGid();
            baseMapper.incrementStats(gid, fullShortUrl, 1, uvFirstFlag.get() ? 1 : 0, uipFirstFlag ? 1 : 0);
            LinkStatsTodayDO linkStatsTodayDO = LinkStatsTodayDO.builder()
                    .todayPv(1)
                    .todayUv(uvFirstFlag.get() ? 1 : 0)
                    .todayUip(uipFirstFlag ? 1 : 0)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkStatsTodayMapper.shortLinkTodayStats(linkStatsTodayDO);
        } catch (Exception e) {
            log.error("短链接访问统计失败，fullShortUrl: {}", fullShortUrl, e);
        }
    }

    private String generateSuffix(LinkCreateReqDTO linkCreateReqDTO) {
        int customGenerateCount = 0;
        String shortUri;
        String fullShortUrl;
        while (true) {
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接生成频繁，请稍后再试");
            }
            String originUrl = linkCreateReqDTO.getOriginUrl();
            originUrl += System.currentTimeMillis();
            shortUri = HashUtil.hashToBase62(originUrl);
            fullShortUrl = StrBuilder.create(createLinkDefaultDomain)
                    .append("/")
                    .append(shortUri)
                    .toString();
            if (!shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl)) {
                break;
            }
            customGenerateCount++;
        }
        return shortUri;
    }

    private String getFavicon(String url) {
        if (StrUtil.isBlank(url)) {
            return null;
        }
        try {
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                url = "http://" + url;
            }
            URL u = new URL(url);
            String protocol = u.getProtocol();
            String host = u.getHost();
            int port = u.getPort();
            StringBuilder base = new StringBuilder()
                    .append(protocol).append("://").append(host);
            if (port > 0 && port != u.getDefaultPort()) {
                base.append(":").append(port);
            }
            String baseUrl = base.toString();

            // 抓取页面 HTML（限 32KB）
            String html = null;
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setConnectTimeout(2000);
                conn.setReadTimeout(2000);
                conn.setInstanceFollowRedirects(true);
                conn.setRequestProperty("User-Agent", "Mozilla/5.0");
                int code = conn.getResponseCode();
                if (code >= 200 && code < 300) {
                    StringBuilder sb = new StringBuilder();
                    try (InputStream in = conn.getInputStream();
                         InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                        char[] buf = new char[4096];
                        int len;
                        int total = 0;
                        while ((len = reader.read(buf)) != -1 && total < 32768) {
                            sb.append(buf, 0, len);
                            total += len;
                        }
                    }
                    html = sb.toString();
                }
            } catch (Exception ignore) {
            }

            // 解析 <link rel="icon"...>
            if (StrUtil.isNotBlank(html)) {
                Pattern p = Pattern.compile("(?i)<link[^>]+rel=[\"'](?:shortcut\\s+)?icon[\"'][^>]*href=[\"']([^\"']+)[\"']");
                Matcher m = p.matcher(html);
                if (m.find()) {
                    String iconHref = m.group(1).trim();
                    if (iconHref.startsWith("http://") || iconHref.startsWith("https://")) {
                        return iconHref;
                    } else if (iconHref.startsWith("//")) {
                        return protocol + ":" + iconHref;
                    } else if (iconHref.startsWith("/")) {
                        return baseUrl + iconHref;
                    } else {
                        return baseUrl + "/" + iconHref;
                    }
                }
            }

            // 回退尝试 /favicon.ico
            String fallback = baseUrl + "/favicon.ico";
            try {
                HttpURLConnection c = (HttpURLConnection) new URL(fallback).openConnection();
                c.setRequestMethod("GET"); // 有些站点不支持 HEAD
                c.setConnectTimeout(1500);
                c.setReadTimeout(1500);
                c.setRequestProperty("User-Agent", "Mozilla/5.0");
                int code = c.getResponseCode();
                String ct = c.getContentType();
                if (code >= 200 && code < 300 && ct != null && ct.toLowerCase().startsWith("image")) {
                    return fallback;
                }
            } catch (Exception ignore) {
            }
        } catch (Exception e) {
        }
        return null;
    }

    private void verificationWhitelist(String originUrl) {
        Boolean enable = gotoDomainWhiteListConfiguration.getEnable();
        if (enable == null || !enable) {
            return;
        }
        String domain = LinkUtil.extractDomain(originUrl);
        if (StrUtil.isBlank(domain)) {
            throw new ClientException("跳转链接填写错误");
        }
        List<String> details = gotoDomainWhiteListConfiguration.getDetails();
        if (!details.contains(domain)) {
            throw new ClientException("演示环境为避免恶意攻击，请生成以下网站跳转链接：" + gotoDomainWhiteListConfiguration.getNames());
        }
    }
}
