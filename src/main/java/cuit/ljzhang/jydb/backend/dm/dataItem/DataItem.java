package cuit.ljzhang.jydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import cuit.ljzhang.jydb.backend.common.SubArray;
import cuit.ljzhang.jydb.backend.dm.DataManagerImpl;
import cuit.ljzhang.jydb.backend.dm.page.Page;
import cuit.ljzhang.jydb.backend.utils.Parser;
import cuit.ljzhang.jydb.backend.utils.Types;

import java.util.Arrays;

/**
 * @ClassName DataItem
 * @Description
 * @Author ljzhang
 * @Date 2023/7/28 19:33
 * @Version 1.0
 * DataItem:DM向上层提供的数据抽象
 *          上层通过地址，向DM请求到对应的DataItem,再获取到其中的数据
 * [ValidFlag][DataSize][Data]
 *          ---ValidFlag:标识DataItem是否有效
 *          ---逻辑删除：有效位置为0
 *          ---DataSize：2字节，标识Data的长度
 */
public interface DataItem {

    /*共享数据  --- 包装*/
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], page, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
