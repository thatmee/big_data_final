# 作用：模拟客户端，向server_host:server_port建立连接并请求推荐列表
# 启动参数：两个
#   - server_host
#   - server_port
import socket
import sys
import pickle

def getArgs():
    argv = sys.argv[1:]
    return argv

def init_socket(host,port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host,port))
    return client_socket

def send_data(cs, datas):
    byteStream = pickle.dumps(datas)
    length = len(byteStream)
    byteStream = bytes(f"{length:<16}", 'utf-8')+byteStream
    cs.sendall(byteStream)

def recv_data(cs):
    msg = cs.recv(1024)
    length = int(msg[:16])
    full_msg = b''
    full_msg += msg[16:]
    nowsize = len(full_msg)
    while nowsize < length:
        more = cs.recv(length - nowsize)
        full_msg = full_msg + more
        nowsize += len(more)
    return pickle.loads(full_msg)

if __name__=="__main__":
    argv = getArgs()
    client_socket = init_socket(argv[0],int(argv[1]))
    while True:
        data = input().strip()
        send_data(client_socket, data)
        response = recv_data(client_socket)
        print(f"Recommend_List:[userid={data}]")
        for i, moview_name in enumerate(response):
            print(f"[Movie-{i+1:<6}: {moview_name}]")
        

