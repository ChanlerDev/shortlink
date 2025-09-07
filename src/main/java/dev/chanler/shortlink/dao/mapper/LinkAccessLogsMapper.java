package dev.chanler.shortlink.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import dev.chanler.shortlink.dao.entity.LinkAccessLogsDO;
import dev.chanler.shortlink.dao.entity.LinkAccessStatsDO;
import dev.chanler.shortlink.dto.req.GroupStatsAccessRecordReqDTO;
import dev.chanler.shortlink.dto.req.GroupStatsReqDTO;
import dev.chanler.shortlink.dto.req.LinkStatsReqDTO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 短链接访问日志持久层
 * @author: Chanler
 */
public interface LinkAccessLogsMapper extends BaseMapper<LinkAccessLogsDO> {

    /**
     * 根据短链接获取指定日期内 PV UV UIP 数据
     * @param linkStatsReqDTO 统计请求参数
     * @return 访问统计数据
     */
    @Select("""
            SELECT
                COUNT(tlal.user) AS pv,
                COUNT(DISTINCT tlal.user) AS uv,
                COUNT(DISTINCT tlal.ip) AS uip
            FROM t_link tl
            INNER JOIN t_link_access_logs tlal ON tl.full_short_url = tlal.full_short_url
            WHERE tlal.full_short_url = #{param.fullShortUrl}
              AND tl.gid = #{param.gid}
              AND tl.del_flag = '0'
              AND tl.enable_status = #{param.enableStatus}
              AND tlal.create_time BETWEEN #{param.startDate} AND #{param.endDate}
            GROUP BY tlal.full_short_url, tl.gid
            """)
    LinkAccessStatsDO findPvUvUidStatsByShortLink(@Param("param") LinkStatsReqDTO linkStatsReqDTO);

    /**
     * 根据分组获取指定日期内PV、UV、UIP数据
     */
    @Select("""
            SELECT
                COUNT(tls.user) AS pv,
                COUNT(DISTINCT tls.user) AS uv,
                COUNT(DISTINCT tls.ip) AS uip
            FROM t_link tl
            INNER JOIN t_link_access_logs tls
                ON tl.full_short_url = tls.full_short_url
            WHERE tl.gid = #{param.gid}
                AND tl.del_flag = '0'
                AND tl.enable_status = '0'
                AND tls.create_time BETWEEN #{param.startDate} AND #{param.endDate}
            GROUP BY tl.gid
            """)
    LinkAccessStatsDO findPvUvUidStatsByGroup(@Param("param") GroupStatsReqDTO groupStatsReqDTO);

    /**
     * 根据短链接获取指定日期内高频访问IP数据
     * @param linkStatsReqDTO 统计请求参数
     * @return 高频访问IP列表
     */
    @Select("""
            SELECT
                tls.ip,
                COUNT(tls.ip) AS count
            FROM t_link tl
            INNER JOIN t_link_access_logs tls
                ON tl.full_short_url = tls.full_short_url
            WHERE tls.full_short_url = #{param.fullShortUrl}
                AND tl.gid = #{param.gid}
                AND tl.del_flag = '0'
                AND tl.enable_status = #{param.enableStatus}
                AND tls.create_time BETWEEN #{param.startDate} AND #{param.endDate}
            GROUP BY tls.full_short_url
                , tl.gid
                , tls.ip
            ORDER BY count DESC
            LIMIT 5
            """)
    List<HashMap<String, Object>> listTopIpByShortLink(@Param("param") LinkStatsReqDTO linkStatsReqDTO);

    /**
     * 根据分组获取指定日期内高频访问IP数据
     * @param groupStatsReqDTO 统计请求参数
     * @return 高频访问IP列表
     */
    @Select("""
            SELECT
                tls.ip,
                COUNT(tls.ip) AS count
            FROM t_link tl
            INNER JOIN t_link_access_logs tls
                ON tl.full_short_url = tls.full_short_url
            WHERE tl.gid = #{param.gid}
                AND tl.del_flag = '0'
                AND tl.enable_status = '0'
                AND tls.create_time BETWEEN #{param.startDate} AND #{param.endDate}
            GROUP BY tl.gid
                , tls.ip
            ORDER BY count DESC
            LIMIT 5
            """)
    List<HashMap<String, Object>> listTopIpByGroup(@Param("param") GroupStatsReqDTO groupStatsReqDTO);

    /**
     * 根据短链接获取指定日期内新旧访客数据
     * @param linkStatsReqDTO 统计请求参数
     * @return 新旧访客统计数据
     */
    @Select("""
            SELECT
                SUM(old_user) AS oldUserCnt,
                SUM(new_user) AS newUserCnt
            FROM (
                SELECT
                    CASE WHEN COUNT(DISTINCT DATE(tls.create_time)) > 1 THEN 1 ELSE 0 END AS old_user,
                    CASE WHEN COUNT(DISTINCT DATE(tls.create_time)) = 1
                        AND MAX(tls.create_time) >= #{param.startDate}
                        AND MAX(tls.create_time) <= #{param.endDate}
                        THEN 1 ELSE 0 END AS new_user
                FROM t_link tl
                INNER JOIN t_link_access_logs tls
                    ON tl.full_short_url = tls.full_short_url
                WHERE tls.full_short_url = #{param.fullShortUrl}
                    AND tl.gid = #{param.gid}
                    AND tl.enable_status = #{param.enableStatus}
                    AND tl.del_flag = '0'
                GROUP BY tls.user
            ) AS user_counts
            """)
    HashMap<String, Object> findUvTypeCntByShortLink(@Param("param") LinkStatsReqDTO linkStatsReqDTO);

    /**
     * 根据短链接和用户列表获取新旧访客类型
     * @param gid 分组标识
     * @param fullShortUrl 完整短链接
     * @param enableStatus 启用状态
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param userAccessLogsList 用户列表
     * @return 用户新旧访客类型列表
     */
    @Select("""
            <script>
            SELECT
                tls.user,
                CASE
                    WHEN MIN(tls.create_time) BETWEEN #{startDate} AND #{endDate} THEN '新访客'
                    ELSE '老访客'
                END AS uvType
            FROM t_link tl
            INNER JOIN t_link_access_logs tls
                ON tl.full_short_url = tls.full_short_url
            WHERE tls.full_short_url = #{fullShortUrl}
                AND tl.gid = #{gid}
                AND tl.del_flag = '0'
                AND tl.enable_status = #{enableStatus}
                AND tls.user IN
                <foreach item='item' index='index' collection='userAccessLogsList' open='(' separator=',' close=')'>
                    #{item}
                </foreach>
            GROUP BY tls.user
            </script>
            """)
    List<Map<String, Object>> selectUvTypeByUsers(
            @Param("gid") String gid,
            @Param("fullShortUrl") String fullShortUrl,
            @Param("enableStatus") Integer enableStatus,
            @Param("startDate") String startDate,
            @Param("endDate") String endDate,
            @Param("userAccessLogsList") List<String> userAccessLogsList
    );

    /**
     * 根据短链接分页获取访问日志
     * @param groupStatsAccessRecordReqDTO 查询参数
     * @return 访问日志分页数据
     */
    @Select("""
            SELECT
                tls.*
            FROM t_link tl
            INNER JOIN t_link_access_logs tls
                ON tl.full_short_url = tls.full_short_url
            WHERE tl.gid = #{param.gid}
                AND tl.del_flag = '0'
                AND tl.enable_status = '0'
                AND tls.create_time BETWEEN #{param.startDate} AND #{param.endDate}
            ORDER BY tls.create_time DESC
            """)
    IPage<LinkAccessLogsDO> selectGroupPage(@Param("param") GroupStatsAccessRecordReqDTO groupStatsAccessRecordReqDTO);

    /**
     * 根据分组和用户列表获取新旧访客类型
     * @param gid 分组标识
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param userAccessLogsList 用户列表
     * @return 用户新旧访客类型列表
     */
    @Select("""
            <script>
            SELECT
                tls.user,
                CASE
                    WHEN MIN(tls.create_time) BETWEEN #{startDate} AND #{endDate} THEN '新访客'
                    ELSE '老访客'
                END AS uvType
            FROM t_link tl
            INNER JOIN t_link_access_logs tls
                ON tl.full_short_url = tls.full_short_url
            WHERE tl.gid = #{gid}
                AND tl.del_flag = '0'
                AND tl.enable_status = '0'
                AND tls.user IN
                <foreach item='item' index='index' collection='userAccessLogsList' open='(' separator=',' close=')'>
                    #{item}
                </foreach>
            GROUP BY tls.user
            </script>
            """)
    List<Map<String, Object>> selectGroupUvTypeByUsers(
            @Param("gid") String gid,
            @Param("startDate") String startDate,
            @Param("endDate") String endDate,
            @Param("userAccessLogsList") List<String> userAccessLogsList
    );
}
