package dev.chanler.shortlink.controller.admin;

import com.baomidou.mybatisplus.core.metadata.IPage;
import dev.chanler.shortlink.common.convention.result.Result;
import dev.chanler.shortlink.common.convention.result.Results;
import dev.chanler.shortlink.dto.req.LinkBatchCreateReqDTO;
import dev.chanler.shortlink.dto.req.LinkCreateReqDTO;
import dev.chanler.shortlink.dto.req.LinkPageReqDTO;
import dev.chanler.shortlink.dto.req.LinkUpdateReqDTO;
import dev.chanler.shortlink.dto.resp.LinkBatchCreateRespDTO;
import dev.chanler.shortlink.dto.resp.LinkCreateRespDTO;
import dev.chanler.shortlink.dto.resp.LinkPageRespDTO;
import dev.chanler.shortlink.service.LinkService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 短链接控制层
 * @author: Chanler
 */
@RestController
@RequiredArgsConstructor
public class LinkAdminController {

    private final LinkService linkService;

    /**
     * 创建短链接
     * @param linkCreateReqDTO 短链接创建请求参数
     * @return Result
     */
    @PostMapping("/api/short-link/admin/v1/create")
    public Result<LinkCreateRespDTO> createLink(@RequestBody LinkCreateReqDTO linkCreateReqDTO) {
        return Results.success(linkService.createLink(linkCreateReqDTO));
    }

    /**
     * 批量创建短链接
     */
    @PostMapping("/api/short-link/admin/v1/create/batch")
    public Result<LinkBatchCreateRespDTO> batchCreateShortLink(@RequestBody LinkBatchCreateReqDTO linkBatchCreateReqDTO) {
        return Results.success(linkService.batchCreateLink(linkBatchCreateReqDTO));
    }

    /**
     * 修改短链接
     * @param linkUpdateReqDTO 短链接更新请求参数
     * @return Result<Void>
     */
    @PostMapping("/api/short-link/admin/v1/update")
    public Result<Void> updateLink(@RequestBody LinkUpdateReqDTO linkUpdateReqDTO) {
        linkService.updateLink(linkUpdateReqDTO);
        return Results.success();
    }

    /**
     * 短链接分页查询
     * @param linkPageReqDTO 分页请求参数
     * @return Result<IPage<LinkPageRespDTO>>
     */
    @GetMapping("/api/short-link/admin/v1/page")
    public Result<IPage<LinkPageRespDTO>> pageLink(LinkPageReqDTO linkPageReqDTO) {
        return Results.success(linkService.pageLink(linkPageReqDTO));
    }
}
