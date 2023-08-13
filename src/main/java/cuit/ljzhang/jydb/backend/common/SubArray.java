package cuit.ljzhang.jydb.backend.common;

/**
 * @ClassName SubArray
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 19:50
 * @Version 1.0
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
