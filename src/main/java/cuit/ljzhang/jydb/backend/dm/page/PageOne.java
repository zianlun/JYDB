package cuit.ljzhang.jydb.backend.dm.page;

import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;
import cuit.ljzhang.jydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @ClassName PageOne
 * @Description
 * @Author ljzhang
 * @Date 2023/7/26 15:46
 * @Version 1.0
 * 数据管理的第一页存储元数据：
 *      --- 启动检测
 *          ---数据库启动时：生成一串随机字节 储存在100~107字节
 *          ---数据库正常关闭：将这段字节拷贝到108~115字节
 *      --- 启动
 *          --- 比较两处字节是否相同
 *                --- 不同，异常关闭，执行数据恢复流程
 */
public class PageOne {
    /*随机字节的存储位置和长度*/
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page){
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    /*随机生成一个8字节长度的随机字节串到 raw*/
    public static void setVcOpen(byte[] raw){
        /*本地方法  ---  快速复制数组*/
        /*raw 的长度为8k*/
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /*关闭系统的时候  需要把随机生成的字节回源到文件中*/
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /*关闭时拷贝字节到  raw对应的位置*/
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /*校验字节*/
    public static boolean checkVc(Page page){
        return checkVc(page.getData());
    }

    /*将raw中数组对应位置的字节内容 和 后面的校验位置进行对比*/
    public static boolean checkVc(byte[] raw){
        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC)
        );
    }


}
