# Etcd-Ops

Etcd-ops 是用于 ETCD 增量备份和恢复的工具。

## 使用方法

### snapshot 子命令

周期性进行全量备份（full-snapshot），全量备份后会监听 etcd event，并将 event 周期性落盘（delta-snapshot）。此方法既可以有效避免频繁全量备份导致
etcd db size 暴涨，又能有效避免 etcd 故障导致长时间的数据丢失，最多丢失一个 delta 时间周期的数据（默认 30s）。

##### 参数说明：

| 参数                           | 说明                | 备注                                           |
|------------------------------|-------------------|----------------------------------------------|
| --bucket                     | 对象存储 bucket 或主机目录 | 使用本机存储时为本机目录                                 |
| --ca-cert                    | etcd ca cert      |                                              |
| --cert                       | etcd server cert  |                                              |
| --cluster-name               | bucket 下的备份目录     |                                              |
| --compression-policy         | 压缩策略              | gzip、zlib or none，enable-compression 默认 gzip |
| --defrag-schedule            | etcd 碎片整理周期策略     | 默认 0 0 */3 * *                               |
| --delta-snapshot-period      | 增量备份间隔            | 默认每 10s 进行一次 etcd event 落盘                   |
| --enable-compression         | 是否开启备份压缩          | 默认不开启                                        |
| --endpoints                  | etcd endpoints    |                                              |
| --etcd-connection-timeout    | etcd 连接超时时间       |                                              |
| --etcd-defrag-timeout        | etcd 碎片整理超时时间     |                                              |
| --etcd-password              | etcd 密码           |                                              |
| --etcd-snapshot-timeout      | etcd 全量备份超时时间     |                                              |
| --etcd-username              | etcd 用户名          |                                              |
| --garbage-collection-period  | 备份清理周期            | 默认每分钟清理一次                                    |
| --garbage-collection-policy  | 备份清理策略            | 保留设定的全量备份还是永久保留                              |
| --insecure-skip-tls-verify   | 跳过服务器证书验证         |                                              |
| --insecure-transport         | 禁用不安全的客户端连接       |                                              |
| --key                        | etcd server key   |                                              |
| --max-backups                | 最多保留多少份全量备份       |                                              |
| --max-parallel-chunk-uploads | 备份上传对象存储的最大并行数    |                                              |
| --min-chunk-size             | 最小块               |                                              |
| --schedule                   | 全量备份周期策略          | */20 * * * *                                 |
| --service-endpoints          | etcd k8s 服务名称     | 暂未使用                                         |
| --storage-provider           | 对象存储提供商           | Local、OSS、S3，默认 Local                        |

##### 本地备份命令示例：

```shell
./bin/etcd-ops snapshot
./bin/etcd-ops snapshot --ca-cert ca.crt --key server.key --cert server.crt --endpoints 192.168.10.51:2379 --insecure-skip-tls-verify false --garbage-collection-policy LimitBased
```

##### 对象存储备份命令示例：

当使用云供应商对象存储时，需提供云服务商认证的环境变量，此环境变量为云服务商密钥保存的路径。比如：当使用阿里云 OSS 时，需提供
ALICLOUD_APPLICATION_CREDENTIALS 环境变量，此变量路径下需存在 accessKeyID、accessKeySecret 和 storageEndpoint 文件。

```shell
export ALICLOUD_APPLICATION_CREDENTIALS=/data
./bin/etcd-ops snapshot --storage-provider OSS --bucket bak-bucket --cluster-name prod-etcd-bak  --enable-compression true
```

### restore 子命令

将对象存储中或本地保存的全量备份和增量备份恢复到 etcd 数据目录。

##### 参数说明：

| 参数                            | 说明                       | 备注                 |
|-------------------------------|--------------------------|--------------------|
| --auto-compaction-mode        | 自动压缩的模式                  |                    |
| --auto-compaction-retention   | 自动压缩的间隔                  |                    |
| --bucket                      | 对象存储 bucket 或主机目录        | 使用本机存储时为本机目录       |
| --cluster-name                | bucket 下的备份目录            |                    |
| --data-dir                    | 恢复后的 etcd 数据目录           |                    |
| --embedded-etcd-quota-bytes   | 用于应用增量备份的嵌入式 etcd 后端存储大小 |                    |
| --initial-advertise-peer-urls | 用户节点间通信的 url             |                    |
| --initial-cluster             | 用于数据恢复的 etcd 集群配置        |                    |
| --initial-cluster-token       | 用于数据恢复的 etcd 集群初始 token  |                    |
| --max-call-send-message-size  | 客户端发送的最大消息               |                    |
| --max-fetchers                | 并行获取增量快照的最大线程数           |                    |
| --max-parallel-chunk-uploads  | 备份上传对象存储的最大并行数           |                    |
| --max-request-bytes           | 服务端接受的最大客户端请求大小          |                    |
| --max-txn-ops                 | 事务中允许的最大操作数              |                    |
| --min-chunk-size              | 最小块                      |                    |
| --name                        | etcd member name         |                    |
| --restore-temp-snapshots-dir  | restore 期间存储备份文件的临时目录    |                    |
| --skip-hash-check             | 忽略 hash 校验               |                    |
| --storage-provider            | 对象存储提供商                  | Local、OSS，默认 Local |

##### 本地数据恢复命令示例：

```shell
./bin/etcd-ops restore
./bin/etcd-ops restore --bucket data-bak --prefix prod-etcd --data-dir /data/restore-data
```

##### 对象存储数据恢复命令示例：

```shell
export ALICLOUD_APPLICATION_CREDENTIALS=/data
./bin/etcd-ops restore --storage-provider OSS --bucket bak-bucket --cluster-name prod-etcd-bak
```

恢复后的数据目录结构

```
default.etcd
└── member
    ├── snap
    │   ├── 0000000000000001-0000000000000001.snap
    │   └── db
    └── wal
        └── 0000000000000000-0000000000000000.wal
```

数据恢复至新的 etcd 集群，参数[链接](https://developer.aliyun.com/article/1094849)

### compact 子命令

将多个增量备份合并为一个全量备份。compact 子命令操作步骤：

- 将备份文件恢复；
- 压缩数据
- 碎片整理
- 进行全量备份
