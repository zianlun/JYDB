package cuit.ljzhang.jydb.backend.dm.pageCache;

import cuit.ljzhang.jydb.backend.dm.page.Page;

/**
 * @ClassName PageCache
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 20:11
 * @Version 1.0
 *
 */
//Todo: 待完成 + 注释
public interface PageCache {

    //页面大小为 8k  --- 2^13字节
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pageNumber) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPagno);

    int getPageNumbers();

    void flushPage(Page page);
}
