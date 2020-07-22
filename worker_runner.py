from worker import Worker
from get_ip_tools import get_ip

worker = Worker(current_worker_addr=(get_ip(),'7998'))
worker.start_server()

