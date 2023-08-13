package cuit.ljzhang.jydb.backend.dm;

import cuit.ljzhang.jydb.backend.common.AbstractCache;
import cuit.ljzhang.jydb.backend.dm.dataItem.DataItem;
import cuit.ljzhang.jydb.backend.dm.dataItem.DataItemImpl;
import cuit.ljzhang.jydb.backend.dm.logger.Logger;
import cuit.ljzhang.jydb.backend.dm.page.Page;
import cuit.ljzhang.jydb.backend.dm.page.PageOne;
import cuit.ljzhang.jydb.backend.dm.page.PageX;
import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;
import cuit.ljzhang.jydb.backend.dm.pageIndex.PageIndex;
import cuit.ljzhang.jydb.backend.dm.pageIndex.PageInfo;
import cuit.ljzhang.jydb.backend.tm.TransactionManager;
import cuit.ljzhang.jydb.backend.utils.Panic;
import cuit.ljzhang.jydb.backend.utils.Types;

import static cuit.ljzhang.jydb.common.Error.DataTooLargeException;
import static cuit.ljzhang.jydb.common.Error.DatabaseBusyException;

/**
 * @ClassName DataManagerImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/7/28 20:10
 * @Version 1.0
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    /*事务管理*/
    TransactionManager tm;

    /*页面缓存*/
    PageCache pc;

    /*日志记录*/
    Logger logger;

    /*页面索引*/
    PageIndex pIndex;

    /*页面*/
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /*
    *从key中解析页号和页内偏移
    * 获取对应的DataItem
    * 页号 + 偏移 = key
    * */
    @Override
    protected DataItem getFoCache(long key) throws Exception {
        /*偏移*/
        short offset = (short) (key & ((1L << 16) - 1));
        key >>>= 32;
        int pageNumber = (int) (key & ((1L << 32) - 1));
        Page page = pc.getPage(pageNumber);
        return DataItem.parseDataItem(page, offset, this);
    }

    /*数据的读写是以页为基本单位的*/
    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    /*创建文件时初始化PageOne*/
    void initPageOne(){
        /*断言不成立抛出 AssertionError异常 */
        int pageNumber = pc.newPage(PageOne.initRaw());
        assert pageNumber == 1;
        try {
            pageOne = pc.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /*初始化页面索引*/
    void fillPageIndex() {
        int pageNumber = pc.getPageNumbers();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    /*打开数据存储文件的第一页PageOne  校验文件的正确性*/
    boolean loadCheckPageOne(){
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /*DM
    *提供三个功能拱上层使用
    * read,insert,update
    * 修改通过读实现
    * */

    /*根据uid从缓存中获取DataItem*/
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl)super.get(uid);
        /*判断是否被删除， 删除的话就释放缓存*/
        if(!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    /*根据xid插入数据*/
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE){
            throw DataTooLargeException;
        }
        PageInfo pageInfo = null;
        for(int i = 0; i < 5; i++){
            pageInfo = pIndex.select(raw.length);
            if(pageInfo != null){
                break;
            }else {
                int newPageNumber = pc.newPage(PageX.initRaw());
                pIndex.add(newPageNumber, PageX.MAX_FREE_SPACE);
            }
        }
        if (pageInfo == null)
            throw DatabaseBusyException;
        Page page = null;
        int freeSpace = 0;
        try {
            page = pc.getPage(pageInfo.pageNumber);
            /*先写入日志*/
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);
            short offset = PageX.insert(page, raw);
            page.release();
            return Types.addressToUid(pageInfo.pageNumber, offset);
        } finally {
            if(page != null){
                pIndex.add(pageInfo.pageNumber, PageX.getFreeSpace(page));
            }else{
                pIndex.add(pageInfo.pageNumber, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /*释放缓存  满足条件则回源*/
    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }

    /*生成更新日志*/
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }
}
