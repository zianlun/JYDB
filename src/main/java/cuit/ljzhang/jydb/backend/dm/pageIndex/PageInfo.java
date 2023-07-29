package cuit.ljzhang.jydb.backend.dm.pageIndex;

/**
 * @ClassName PageInfo
 * @Description
 * @Author ljzhang
 * @Date 2023/7/27 23:44
 * @Version 1.0
 * 为页面索引所使用
 *      --- 存储了页面编号和剩余内存空间
 *      --- 方便上层模块快速查找到合适的页面
 */
public class PageInfo {
    public int pageNumber;
    public int freeSpace;

    public PageInfo(int pageNumber, int freeSpace) {
        this.pageNumber = pageNumber;
        this.freeSpace = freeSpace;
    }
}