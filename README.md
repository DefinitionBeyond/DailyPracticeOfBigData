本项目主要记录个人学习大数据，编程的练习

>1 .目录结构：

    
    src
    
            - HDFS   对hdfs文件的操作
                 
            - Hbase    hbase编程，对hbase数据库进行操作
            
            - MapReduce    mapreduce编程
            
            - HbasendAMapReduce    mapreduce编程和hbase编程结合，对hbase数据库操作，或者对hdfs文件操作


> 2 .执行参考

MapReduce: [https://blog.csdn.net/weijifeng_/article/details/79779419](https://blog.csdn.net/weijifeng_/article/details/79779419)

Hbase:[https://blog.csdn.net/weijifeng_/article/details/79873492](https://blog.csdn.net/weijifeng_/article/details/79873492)

hdfs:需要导入/opt/hadoop/share/hadoop/common下的jar以及/opt/hadoop/share/hadoop/common/lib下的jar

>3 .运行数据集

对于运行数据集，可以根据数据格式对代码进行一定的修改

对自己选取的目的进行数据集操作

>4 .编程注意事项

maper过程读取文件是按行读取的

自定义context输出key - value的格式

```
		conf.set("mapred.textoutputformat.ignoreseparator","true");  
		conf.set("mapred.textoutputformat.separator",","); 
```

不使用hbase自带的zookeeper集群

```
cfg.set("hbase.zookeeper.quorum", "master，slave，slavex");
```




