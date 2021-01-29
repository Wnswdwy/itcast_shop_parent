package com.itcast.canal_client.app;

/**
 * @author yycstart
 * @create 2021-01-27 19:39
 *
 * canal客户端的入口类
 */
public class Entrance {
    public static void main(String[] args) {
        //实例话canal客户端对象，调用start方法拉取canalServer端的binlog日志
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}
