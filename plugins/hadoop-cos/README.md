# HADOOP-COS
v8.0.5改版，增加了对Amazon EMR 6系列的支持，可以自己编译，也可以使用打包好的jar包，cos api的jar包也放在一起了

## 功能说明
Hadoop-COS实现了以腾讯云 COS 作为底层文件系统运行上层计算任务的功能，支持使用Hadoop、Spark以及Tez等处理存储在腾讯云COS对象存储系统上的数据。

## 使用限制
只适用于 COS V5 版本

## 使用环境
### 软件依赖
已打包Amazon EMR需要的Hadoop-3.2.1版本
Hadoop-2.6.0及以上版本
如果各位希望使用EMR 5系列，可能需要自己打包相关版本，因为后期版本的EMR 5系列使用了比较少见的Hadoop 2.10版本；
建议各位还是使用EMR 6系列，因为Hadoop 3.0开始，除了AWS以外，Aliyun和Azure的对象存储也在默认支持列表里，不需要额外安装jar包了；
希望Amazon EMR早日升级到Hadoop 3.3；

**NOTE**：
1. 目前hadoop-cos已经正式被Apache Hadoop-3.3.0官方集成：[https://hadoop.apache.org/docs/r3.3.0/hadoop-cos/cloud-storage/index.html](https://hadoop.apache.org/docs/r3.3.0/hadoop-cos/cloud-storage/index.html)。
2. 在Apache Hadoop-3.3.0 之前版本或CDH集成Hadoop-cos jar 包后，需要重启NameNode才能加载到jar包。
3. Amazon EMR上暂时未观察到需要重启NameNode的问题，仅供参考。

## 安装方法

### 安装hadoop-cos

1. 将hadoop-cos-{hadoop.version}-x.x.x.jar和cos_api-bundle-5.x.x.jar 拷贝到 `/usr/lib/hadoop/`和`/usr/lib/hadoop-mapreduce/`下。

## 使用方法

### HADOOP配置

修改 $HADOOP_HOME/etc/hadoop/core-site.xml，增加 COS 相关用户和实现类信息，下列是必须项：

```xml
<configuration>

    <property>
        <name>fs.cosn.credentials.provider</name>
        <value>org.apache.hadoop.fs.auth.SimpleCredentialProvider</value>
        <description>

            This option allows the user to specify how to get the credentials.
            Comma-separated class names of credential provider classes which implement
            com.qcloud.cos.auth.COSCredentialsProvider:

            1.org.apache.hadoop.fs.auth.SessionCredentialProvider: Obtain the secretId and secretKey from the URI:cosn://secretId:secretKey@example-1250000000000/;
            2.org.apache.hadoop.fs.auth.SimpleCredentialProvider: Obtain the secret id and secret key
            from fs.cosn.userinfo.secretId and fs.cosn.userinfo.secretKey in core-site.xml;
            3.org.apache.hadoop.fs.auth.EnvironmentVariableCredentialProvider: Obtain the secret id and secret key
            from system environment variables named COS_SECRET_ID and COS_SECRET_KEY.

            If unspecified, the default order of credential providers is:
            1. org.apache.hadoop.fs.auth.SessionCredentialProvider
            2. org.apache.hadoop.fs.auth.SimpleCredentialProvider
            3. org.apache.hadoop.fs.auth.EnvironmentVariableCredentialProvider
            4. org.apache.hadoop.fs.auth.SessionTokenCredentialProvider
            5. org.apache.hadoop.fs.auth.CVMInstanceCredentialsProvider
            6. org.apache.hadoop.fs.auth.CPMInstanceCredentialsProvider
            7. org.apache.hadoop.fs.auth.EMRInstanceCredentialsProvider
        </description>
    </property>

    <property>
        <name>fs.cosn.userinfo.secretId</name>
        <value>xxxxxxxxxxxxxxxxxxxxxxxxx</value>
        <description>Tencent Cloud Secret Id</description>
    </property>

    <property>
        <name>fs.cosn.userinfo.secretKey</name>
        <value>xxxxxxxxxxxxxxxxxxxxxxxx</value>
        <description>Tencent Cloud Secret Key</description>
    </property>

    <property>
        <name>fs.cosn.bucket.region</name>
        <value>ap-shanghai</value>
        <description>The region where the bucket is located，此处配置了上海区</description>
    </property>

    <property>
        <name>fs.cosn.bucket.endpoint_suffix</name>
        <value>cos.ap-shanghai.myqcloud.com</value>
        <description>COS endpoint to connect to.
        For public cloud users, it is recommended not to set this option, and only the correct area field is required.</description>
    </property>

    <property>
        <name>fs.cosn.impl</name>
        <value>org.apache.hadoop.fs.CosFileSystem</value>
        <description>The implementation class of the CosN Filesystem</description>
    </property>

    <property>
        <name>fs.AbstractFileSystem.cosn.impl</name>
        <value>org.apache.hadoop.fs.CosN</value>
        <description>The implementation class of the CosN AbstractFileSystem.</description>
    </property>

    <property>
        <name>fs.cosn.tmp.dir</name>
        <value>/tmp/hadoop_cos</value>
        <description>Temporary files will be placed here.</description>
    </property>

    <property>
        <name>fs.cosn.upload.buffer</name>
        <value>mapped_disk</value>
        <description>The type of upload buffer. Available values: non_direct_memory, direct_memory, mapped_disk</description>
    </property>

    <property>
        <name>fs.cosn.upload.buffer.size</name>
        <value>33554432</value>
        <description>The total size of the upload buffer pool. -1 means unlimited.</description>
    </property>

    <property>
        <name>fs.cosn.upload.part.size</name>
        <value>8388608</value>
        <description>The part size for MultipartUpload.
        Considering the COS supports up to 10000 blocks, user should estimate the maximum size of a single file.
        For example, 8MB part size can allow  writing a 78GB single file.</description>
    </property>

    <property>
        <name>fs.cosn.maxRetries</name>
        <value>3</value>
        <description>The maximum number of retries for reading or writing files to
    COS, before we signal failure to the application.</description>
    </property>

    <property>
        <name>fs.cosn.retry.interval.seconds</name>
        <value>3</value>
        <description>The number of seconds to sleep between each COS retry.</description>
    </property>

    <property>
        <name>fs.cosn.read.ahead.block.size</name>
        <value>‭1048576‬</value>
        <description>
            Bytes to read ahead during a seek() before closing and
            re-opening the cosn HTTP connection.
        </description>
    </property>

    <property>
        <name>fs.cosn.read.ahead.queue.size</name>
        <value>8</value>
        <description>The length of the pre-read queue.</description>
    </property>

</configuration>

```

**配置项说明**：
参考hadoop-cos官方github
https://github.com/tencentyun/hadoop-cos