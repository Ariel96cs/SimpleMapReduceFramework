import file_system as fs
from get_ip_tools import get_ip

f = fs.FileSystem(ip=get_ip(),port='8080',addr_recv_broadcast=(get_ip(),'8081'))
print("FILESYSTEM:out of the register")
f.start_server()
