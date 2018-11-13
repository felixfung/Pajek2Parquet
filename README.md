# Pajek2Parquet

Spark program to convert (serial) Pajek net into distributed Parquet files, using the GraphFrame library.

The Pajek file is a single serial file.

The Parquet file consists of a vertices file, and an edge file, that is linked together by a json file.

The json file has content:

```
{
    "Vertex File": "verticesFilename",
    "Edge File": "edgesFilename"
}
```

## How to use

Below is simple instruction to run this in a local cluster

### Set up HDFS

```
cd "path of hadoop"
bin/hdfs namenode -format
./sbin/start-dfs.sh
```

If connection is refused, check if ssh service is set up.

Detailed documentation is found in http://hadoop.apache.org/docs/stable/

### Set up hdfs directory

```
hadoop fs -mkdir /pqt
hadoop fs -ls /pqt
```

### spark-submit

```
spark-submit --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pajek2parquet_2.11-1.0.jar pajek.net json.parquet hdfs:/pqt/vertices.parquet hdfs:/pqt/edges.parquet
```
