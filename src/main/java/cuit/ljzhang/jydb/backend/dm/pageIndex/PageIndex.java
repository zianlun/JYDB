package cuit.ljzhang.jydb.backend.dm.pageIndex;

import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;

import java.util.List;

/**
 * @ClassName PageIndex
 * @Description
 * @Author ljzhang
 * @Date 2023/7/27 22:13
 * @Version 1.0
 * 页面索引：
 *      缓存每一页的空闲空间
 *      在上层模块进行插入操作时，能够快速找到一个合适空间的页面
 */
public class PageIndex {
    /*将页的数据空间大小按顺序划分为四个区间*/
    private static final int INTERVALS_NO = 40;
    /* 8k / 4 = 1024 * 8 / 40 = 2048 / 10 = 204.8*/
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    /*四十个区间*/
    private List<PageInfo>[] lists;

    /* 根据需要的空间大小计算出 需要的页面区间 快速获取页面 */
    public PageInfo select(int spaceSize){
        int number = spaceSize / THRESHOLD;
        /*向上取整 保证页的剩余空间大小能写入数据*/
        if(number < INTERVALS_NO)number ++;
        while(number <= INTERVALS_NO){
            //如果指定的数据空间没有页面  那么就需要选剩余空间更大的页面
            if(lists[number].size() == 0){
                number++;
                continue;
            }
            return lists[number].remove(0);
        }
        return null;
    }

    /*上层模块使用完我们的页面后 会将页面插入回来*/
    public void add(int pageNumber, int freeSpace){
        int number = freeSpace / INTERVALS_NO;
        lists[number].add(new PageInfo(pageNumber, freeSpace));
    }


}
