package cuit.ljzhang.jydb.backend.dm;

import cuit.ljzhang.jydb.backend.dm.dataItem.DataItem;
import cuit.ljzhang.jydb.backend.dm.logger.Logger;
import cuit.ljzhang.jydb.backend.dm.page.PageOne;
import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;
import cuit.ljzhang.jydb.backend.tm.TransactionManager;

/**
 * @ClassName DataManager
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 19:53
 * @Version 1.0
 *
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /*
    * 创建空文件
    * 对第一页空间进行初始化
    * Todo: memory
    *  */
    public static DataManager create(String path, long memory, TransactionManager tm) {
        PageCache pc = PageCache.create(path, memory);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    /*
    * dm的初始化工作
    * 初始化页面缓存
    * 初始化日志
    * */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        /*数据校验不通过  --- 执行日志恢复策略*/
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
