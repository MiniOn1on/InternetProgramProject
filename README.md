# InternetProgramProject
进阶挑战性课程Linux,socket网络编程项目及对应软件工程工作仓库
## 心跳保活及业务实现
本仓库共采用三种方法来实现及维护轻量级云边端框架中客户端（ECUApp）与服务端（ECUAdmin）的`websocket`长连接，主要采用心跳保活机制，python多线程`threading timer`版本，`sleep`版本，以及`async异步`版本，接着再以异步为基础加入了业务数据参与，主要数据可分为：
1. 心跳数据
2. 业务数据透传（本仓库采用30s周期发送，实际可触发式发送）
3. 状态数据周期性上报
4. 云端命令下发

|实现方法|具体位置|
|---|---|
|threading timer|[Threading timer版目录](./Threadingtimer版本)|
|sleep|[sleep版目录](./sleep版本)|
|async|[async版目录](./async版ServerToClient通信机制(含业务))|
