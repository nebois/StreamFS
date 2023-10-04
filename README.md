# StreamFS
> 一个简单的分布式文件系统实现

## 项目结构
> 项目提供了编译完成的示例文件可部署使用, 在workpublish目录下, 需要Zookeeper服务支持
- bin：项目一键启动脚本，用于编译完成后，上传至服务器上，可以将minFS服务整体启动起来
- dataServer:主要提供数据内容存储服务能力，单节点无状态设计，可以横向扩容
- metaServer:主要提供文件系统全局元数据管理，管理dataserver的负载均衡，主备模式运行
- easyClient:一个功能逻辑简单的SDK，用来与metaServer、dataServer通信，完成文件数据操作
