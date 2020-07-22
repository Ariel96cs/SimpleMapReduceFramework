import zmq
from multiprocessing import Process
from threading import Thread
import os
from tasktracker import TaskTracker
from jobtracker import JobTracker
import message_tools as mt
from sys import exit
import hashlib


class Worker:
    def __init__(self,
                 current_worker_addr=('0.0.0.0','7998'),
                 limit_task=5,job_limit=2,
                 worker_broadcast_addr=('10.255.255.255','7999'),
                 filesystem_broadcast_addr=('10.255.255.255','8081')
                 ):
        self.names = {
            'DONE': self.free_capacity,
            'TASK': self.execute_task,
            'JOB': self.execute_job,
            'STATUS': self.answer_status,
            'JOBREG': self.job_reg
        }
        self.filesystem_broadcast_addr = filesystem_broadcast_addr
        self.current_worker_addr = current_worker_addr
        self.file_sys_addrs = []
        self.limit_task = limit_task
        self.job_limit = job_limit
        self.current_task_cap = limit_task
        self.current_job_cap = job_limit
        self.worker_broadcast_addr = worker_broadcast_addr
        try:
            os.mkdir('mr_modules')
        except FileExistsError:
            print('Worker: ',"Ya existe el directorio module")
        with open('./mr_modules/__init__.py','a') as f:
            print('Worker: ',"se crea el archivo __init__.py")

    @staticmethod
    def _job_id(files: list, extra: str):
        """
        Gives an ID to an incoming job. Is a personal hash.

        :param files:
        The list of urls of the job's files on the client
        :param client_addr:
        The address of the client
        :return:
        The job's ID
        """
        files_str = ""
        for file in files:
            files_str += file
        job_id = hashlib.sha1(files_str.encode() + extra.encode()).hexdigest()
        return job_id

    def job_reg(self,socket,message):
        if "files" in message.payload and "extra_info" in message.payload and len(message.payload["files"]) > 0:
            extra = message.payload["extra_info"]
            job_id = self._job_id(message.payload["files"], extra)
            posible_trackers = self.get_workers()
            self.update_filesystem_nodes()
            if len(posible_trackers) == 0:
                print("Worker: ",'Se cayeron todos los workers')
                socket.send_string(str(mt.Message().set_message("ERROR", {"info": "No Workers availables!!!"
                                                                                      })))
                return -1

            socket.send_string(str(mt.Message().set_message("OK", {"info": "Job correctly registered",
                                                                "job_id": job_id,
                                                                "trackers_addr": posible_trackers,
                                                                "filesystem": self.file_sys_addrs})))
            print("OK message sent")

        else:
            socket.send_string(str(mt.Message().set_message("ERROR", {"info": "Incorrect job format. Any job must have the"
                                                                           " client listening address on the \"addr\""
                                                                           " field and the 2 files required: the"
                                                                           " functions file, and the data file on an"
                                                                           " array on the \"files\" field."})))

    def answer_status(self,socket,message):

        answer = mt.Message().set_message("OK",{
            'current_task_cap': self.current_task_cap,
            'current_job_cap': self.current_job_cap
        })
        socket.send_string(str(answer))

    def get_workers(self):
        print("Worker: ", "Buscamos por otros workers")
        workers = mt.send_broadcastmessage(self.worker_broadcast_addr)
        print("Worker: ",'Estos son los workers disponibles: ',workers)
        return workers

    def free_capacity(self,socket,message):
        print('Worker: ', "Let's free capacity!!!: ")
        if message.payload['role'] == 'task':
            if self.current_task_cap < self.limit_task:
                self.current_task_cap += 1
        else:
            if self.current_job_cap < self.limit_task:
                self.current_job_cap += 1
        print('Worker: ', "This is my current task capacity: ", self.current_task_cap)
        print('Worker: ', "This is my current job capacity: ", self.current_job_cap)

        socket.send_string(str(mt.Message().set_message("OK")))

    # Todo: working for removing register
    def update_filesystem_nodes(self):
        print("Worker: ", 'Mandamos a buscar los nodos del filesystem')
        filesystem_nodes = mt.send_broadcastmessage(self.filesystem_broadcast_addr)
        print("Worker: ", "Estos son los nodos del filesystem: ",filesystem_nodes)
        self.file_sys_addrs = filesystem_nodes
        if len(filesystem_nodes) == 0:
            print("Worker: ",'no me respondieron los nodos del filesystem')
            exit()
        return 0

    def register_worker(self):
        ping_addr = (self.current_worker_addr[0],self.worker_broadcast_addr[1])
        thread = Thread(target=self._answering_ping,name='worker_pinging',args=(ping_addr,))
        thread.daemon = True
        thread.start()

    def _answering_ping(self,ping_addr):
        return mt.recv_ping_broadcast(ping_addr,self.current_worker_addr)

    def execute_job(self,socket,message):
        if self.current_job_cap == 0:
            print("Worker: Ups! NO capacity for more tasks")
            answer = mt.Message().set_message('ERROR', {'info': "Worker busy", 'type_error': 'WorkerBusyError'})
            socket.send_string(str(answer))
            return None
        self.current_job_cap -= 1
        arguments = message.payload
        tracker_ip = self.current_worker_addr[0]
        tracker_addr_ping = (tracker_ip,mt.get_available_random_port())
        tracker = JobTracker(self.worker_broadcast_addr,
                             self.filesystem_broadcast_addr,
                             tracker_addr_ping,
                             tracker_ip,
                             self.current_worker_addr, **arguments)
        f = lambda track: track.execute_job()

        procces = Process(target=f, args=(tracker,),name='worker_procces_job')
        procces.start()
        print('Worker: ',"submited job")
        answer = mt.Message().set_message('OK',{"ping_tracker_ip": tracker_addr_ping[0],
                                                "ping_tracker_port": tracker_addr_ping[1]
                                                })
        socket.send_string(str(answer))
        print('Worker: ', "This is my current job capacity: ", self.current_job_cap)

    def execute_task(self, socket, message):
        if self.current_task_cap == 0:
            print("Worker: Ups! NO capacity for more tasks")
            answer = mt.Message().set_message('ERROR',{'info':"Worker busy",'type_error':'WorkerBusyError'})
            socket.send_string(str(answer))
            return None
        print("Worker: ",'Vamos a ejecutar un task')

        self.current_task_cap -= 1
        arguments = message.payload
        current_ip = self.current_worker_addr[0]
        task_ping_addr = (current_ip,mt.get_available_random_port(current_ip))

        task_executor = TaskTracker(self.filesystem_broadcast_addr,
                                    task_ping_addr,
                                    self.current_worker_addr,
                                    **arguments)

        f = lambda task_ex: task_ex.execute_task()

        procces = Process(target=f,args=(task_executor,),name='worker_task_proccess')
        print('Worker: ',"submited task")
        procces.start()
        answer = mt.Message().set_message('OK',{'task_ping_ip':task_ping_addr[0],'task_ping_port':task_ping_addr[1]})
        socket.send_string(str(answer))
        print('Worker: ', "This is my current task capacity: ",self.current_task_cap)

    def start_server(self):
        '''
        Escucha lo que se manda por addr y en dependencia del mensaje, se ejecuta la función necesaria

        :return:
            Nothing
        '''
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #
        # ip = self.current_worker_addr[0]
        # socket, port = mt.bind_to_random_port(socket,ip)
        # self.current_worker_addr = (ip,port)
        socket.bind('tcp://{}:{}'.format(*self.current_worker_addr))
        print('Worker: ',"Running on {}".format(self.current_worker_addr))

        #tratar de que hasta que no me registre no ponerme a escuchar
        self.register_worker()
        result_ = self.update_filesystem_nodes()
        if result_ == -1:
            return -1
        print('Worker: Actualizamos los nodos del filesystem: ',self.file_sys_addrs)

        while True:
            print('Worker: ',"Esperando por comando el worker: {}:{}".format(*self.current_worker_addr))
            text = socket.recv_string()
            print('Worker: ','Me llego un mensaje: ',text)
            message = mt.Message()
            message.get_message(text)
            function_to_execute = self.names[message.message_name]
            function_to_execute(socket, message)
