package cn.itcast.shop.realtime.etl.app

import cn.itcast.shop.realtime.etl.process.SyncDimData
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @author yycstart
 * @create 2021-01-28 17:07
 *
 *        创建实时ETL模块放入驱动类，实现所有的ETL业务
 */

object App {
  /**
   * 入口函数
   */
  def main(args: Array[String]): Unit = {
    /**
     * 实现步骤：
     * 1. 初始化Flink流式运行环境
     * 2. 设置Flink的并行度，生产环境需要注意，尽可能client递交作业的时候指定并行度
     * 3. 开启Flink的checkpoint
     * 4. 接入Kafka的数据源，消费Kafka的数据
     * 5. 实现所有的ETL任务
     * 6. 执行任务
     */

    //TODO 1: 初始化Flink的流式处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO 2: 设置Flink的并行度为 1 , 测试环境为 1 ，生产环境需要住，尽可能使用client递交作业的时候指定并行度
    env.setParallelism(1);

    //TODO 3: 开启Flink的Checkpoint
    //开启checkpoint的时候，设置checkpoint的运行周期，每隔5秒钟进行一次checkpoint
    env.enableCheckpointing(5000)
    //当作业被cancel的时候，保留以前的checkpoint，避免数据的丢失
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置同一个时间只能有一个检查点，检查点的操作是否可以并行，1不能并行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的HDFS保存位置
    env.setStateBackend(new FsStateBackend("hdfs://hadoop02:8020/flink/checkpoint/"))
    // 配置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 配置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    //指定重启策略，默认的是不停的重启
    //程序出现异常的时候，会进行重启，重启五次，每次延迟5秒钟，如果超过了五次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000))

//    // 4. 编写测试代码，测试Flink程序是否能够正确执行
//    env.fromCollection(
//      List("hadoop", "hive", "spark")
//    ).print()

    //TODO 5： 实现所有的 ETL 业务

    // 5.1 维度表的数据增量到Redis中
    val syncDataProcess: SyncDimData = new SyncDimData(env)
    syncDataProcess.process()

    //TODO 6：执行任务
    env.execute()
  }
}
