package cuit.ljzhang.jydb.backend.dm.page;

import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;
import cuit.ljzhang.jydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @ClassName PageX
 * @Description
 * @Author ljzhang
 * @Date 2023/7/26 21:45
 * @Version 1.0
 * PageX 管理普通页
 * [FreeSpaceOffset][Data]
 * FreeSpaceOffset: 2字节 空闲位置的偏移
 *      ---普通页的操作主要是围绕着FSO进行的
 *      ---向页面插入数据
 *      ---
 */
public class PageX {

    /*偏移和数据的位置*/
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /*初始化缓存页*/
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    /*
    * 向缓存页插入数据
    * Page将要插入数据的缓存页
    * raw是待插入的数据
    * 返回插入位置
    * */
    public static short insert(Page page, byte[] raw){
        page.setDirty(true);
        short offset = getFSO(page.getData());
        /*从空闲的偏移位置开始将raw复制到page*/
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short)(offset + raw.length));
        return offset;
    }

    /*将偏移数据存入fso中*/
    public static void setFSO(byte[] raw,  short fso){
        System.arraycopy(Parser.short2Byte(fso), 0,
                raw, OF_FREE, OF_DATA);
    }

    /*获取[FreeSpaceOffset]*/
    public static short getFSO(Page page){
        return getFSO(page.getData());
    }

    /*获取空闲页的偏移位置*/
    public static short getFSO(byte[] raw){
        /*byte[]转换为short类型*/
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2)) ;
    }

    /*获取页面的空间空间大小*/
    public static int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE - (int)getFSO(page);
    }

    /*数据崩溃之后  重启恢复插入和更新数据*/
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset + raw.length));
        }
    }

    /*
    * 将缓存页设置为脏页：
    *       然后重写内容进缓存页
    *       redo的一个更新操作
    * */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }

}
