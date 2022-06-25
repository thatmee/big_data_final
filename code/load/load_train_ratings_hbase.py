# 作用：向redis中放入movieId2movieName
# 启动参数：四个
#   - hbase_host
#   - hbase_thrift_port
#   - table_name
#   - file_path
import happybase
import json
import sys

def getArgs():
    argv = sys.argv[1:]
    return argv[0], int(argv[1]), argv[2], argv[3]

def get_trst(file_path):
    with open(file_path,"r") as file:
        line = file.readline()
        while line:
            yield json.loads(line.strip("\n"))
            line = file.readline()

def trans2hbaseRow(record:dict):
    new_record = dict()
    new_record['rowkey'] = record["headers"]["key"]
    new_record['cells'] = {} 
    for k,v in json.loads(record["body"]).items():
        print(k,v)
        new_record['cells'][f"details:{k}"] = str(v)
    print(new_record)
    return new_record


if __name__=="__main__":
    host, port, table_name, file_path = getArgs()
    hbase_connection = happybase.Connection(host=host,port=port,timeout=None,autoconnect=True,table_prefix=None,table_prefix_separator=b'_',compat='0.98', transport='buffered',protocol='binary')
    table = happybase.Table(table_name,hbase_connection)
    for record in get_trst(file_path):
        record = trans2hbaseRow(record)
        table.put(record['rowkey'],record['cells'])