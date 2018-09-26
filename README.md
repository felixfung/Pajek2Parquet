# Pajek2Parquet

Spark program to convert (serial) Pajek net into distributed Parquet files, using the GraphFrame library.

The Pajek file is a single serial file.

The Parquet file consists of a vertices file, and an edge file, that is linked together by a json file.

The json file has content:

{
    "Vertex File": "verticesFilename",
    "Edge File": "edgesFilename"
}
