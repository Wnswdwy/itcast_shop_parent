package cn.itcast.canal.protobuf;

/**
 * @author yycstart
 * @create 2021-01-28 13:14
 *
 * 定义protobuf接口
 * 接口定义的是返回的byte【】二进制字节码对象
 * 意味着所有能够使用protobuf序列化的bean对象都需要集成该接口
 */
public interface ProtoBufable {
    /**
     * 将对象转换成二进制数组
     * @return
     */
    byte[] toBytes();
}
