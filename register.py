from asyncio import sleep
import time
from message_parser_protocol import Message
from random import Random
import hashlib
import asyncio
import zmq
import zmq.asyncio
from threading import Thread
# from multiprocessing import Process


class Register:

    def __init__(self, ip="0.0.0.0", port="9090"):
        self.commands = {
            "WORKERREG": self.worker_register,
            "JOBREG": self.job_register,
            "FILESYSREG": self.filesys_register,
            "WORKERS": self.workers,
            "FILESYSNODES": self.filesys_nodes,
        }
        self._registered_workers = []
        self._jobs = set()
        self._filesystem = []
        self.ip = ip
        self.port = port
        self.loop = asyncio.get_event_loop()

    def worker_register(self, message, socket):
        """
        Registers a worker in the system

        :param message:
        A message already instantiated with the class Message from the message_parsing_protocol module
        :param socket:
        The socket who brought the message
        :return:
        """
        print("REG: Into worker register")
        addrs = (message.payload["worker_addr"], message.payload["ping_addr"])
        answer_message = Message()
        if addrs not in self._registered_workers:
            self._registered_workers.append(addrs)
            answer_message.set_message("OK", {"info": "Registration completed"})
        else:
            answer_message.set_message("OK", {"info": "You were already registered"})
        socket.send_string(str(answer_message))
        print("REG: registered workers:", self._registered_workers)

    @staticmethod
    async def _ping_worker(addr):
        print("PINGING TO:", addr)
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.REQ)
        try:
            print("REG:" + str(addr))
            socket.connect("tcp://" + addr[0] + ":" + addr[1])
            socket.send_string(str(Message().set_message("PING", {"info": "estás vivo?"})))
        except(ConnectionError, ConnectionAbortedError, ConnectionRefusedError, ConnectionResetError):
            print("REG:" + "ZMQ failed?!")

        try:
            answer = await socket.recv_string()
            print("REG:" + "PING answer:", answer)
            answer = Message().get_message(answer)
            print("REG:" + str(answer))
            if answer.message_name == "OK":
                socket.close()
                return

        except asyncio.TimeoutError:
            print("REG:" + "Ni PING")
            socket.close()

        return

    def _update_workers(self):
        # extra_filesystem_list = []

        while True:
            print("REG: Workers to ping:", self._registered_workers + self._filesystem)
            # loop = asyncio.get_event_loop()
            for i, service_node in enumerate(self._registered_workers + self._filesystem):
                try:
                    # print("REG:Now need to ping:", i, worker)
                    ping = self.loop.run_until_complete(asyncio.wait_for(self._ping_worker(service_node[1]), 2.0))
                    # print(ping)
                    # print("REG:Out of the ping")

                except asyncio.TimeoutError:
                    if service_node in self._registered_workers:
                        print("REG: worker", service_node, "removed")
                        self._registered_workers.remove(service_node)
                    elif service_node in self._filesystem:
                        print("REG: node", service_node, "removed")
                        self._filesystem.remove(service_node)
                        # continue

            time.sleep(5.0)
            print("REG: registered workers after update" + str(self._registered_workers))
            print("REG: registered nodes after update" + str(self._filesystem))

    def _get_all_workers(self):
        return self._registered_workers

    def _get_trackers_addr(self):
        return self._get_all_workers()

    def _get_filesystem_addr(self):
        return self._filesystem[Random().randint(0, len(self._filesystem) - 1)]

    def filesys_nodes(self, message, socket):
        socket.send_string(str(Message().set_message("OK", {"nodes":[node[0] for node in self._filesystem]})))

    def filesys_register(self, message, socket):
        """
        Registers a node of the filesystem

        :param message:
        :param socket:
        :return:
        """
        addr = message.payload["addr"]
        ping_addr = message.payload["ping_addr"]
        answer_message = Message()
        if addr not in self._filesystem:
            self._filesystem.append((addr, ping_addr))
            answer_message.set_message("OK", {"info": "Registration completed"})
        else:
            answer_message.set_message("OK", {"info": "You were already registered"})
        socket.send_string(str(answer_message))

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

    def job_register(self, message, socket):
        if "files" in message.payload and "extra_info" in message.payload and len(message.payload["files"]) > 0:
            addr = message.payload["extra_info"]
            job_id = self._job_id(message.payload["files"], addr)
            tracker = self._get_trackers_addr()

            if len(tracker) == 0 or len(self._filesystem) == 0:
                socket.send_string(str(Message().set_message("ERROR", {"info": "There's no worker connected."
                                                                               "Retry soon."})))
                return

            if job_id in self._jobs:
                socket.send_string(str(Message().set_message("OK", {"info": "That job is already registered",
                                                                    "job_id": job_id,
                                                                    "trackers_addr": tracker,
                                                                    "filesystem": [node[0] for node in self._filesystem]
                                                                    })))
                return
            # self._jobs[message.payload["files"]+addr[0]+addr[1]] = job_id
            self._jobs.add(job_id)
            # print("REG: Estoy aqui")
            socket.send_string(str(Message().set_message("OK", {"info": "Job correctly registered",
                                                                "job_id": job_id,
                                                                "trackers_addr": tracker,
                                                                "filesystem": [node[0] for node in self._filesystem]})))
            print("OK message sent")

        else:
            socket.send_string(str(Message().set_message("ERROR", {"info": "Incorrect job format. Any job must have the"
                                                                           " client listening address on the \"addr\""
                                                                           " field and the 2 files required: the"
                                                                           " functions file, and the data file on an"
                                                                           " array on the \"files\" field."})))

    # def _ping_filesystem_nodes(self):
    #     while True:
    #         print("REG: Filesystem nodes to ping:", self._filesystem)
    #         # loop = asyncio.get_event_loop()
    #         for i, node in enumerate(self._filesystem):
    #             try:
    #                 print("REG:Now need to ping:", i, node)
    #                 ping = self.loop.run_until_complete(asyncio.wait_for(self._ping_worker(node[1]), 2.0))
    #                 print("Filesystem Ping:", ping)
    #                 print("REG:Out of the ping")
    #             except asyncio.TimeoutError:
    #                 print("REG: Node", self._filesystem[i], "removed")
    #                 self._filesystem.remove(self._filesystem[i])
    #                 continue
    #         print("REG: registered after update" + str(self._filesystem))
    #         # sleep(1.0)

    def workers(self, message, socket):
        socket.send_string(str(Message().set_message("OK", {"workers": self._registered_workers})))

    def start_server(self):
        context = zmq.Context()
        message_socket = context.socket(zmq.REP)
        message_socket.bind("tcp://" + self.ip + ":" + self.port)

        registered_workers_updater = Thread(None, self._update_workers)
        registered_workers_updater.daemon = True
        print("REG: About to start the pinging")
        registered_workers_updater.start()

        message = Message()
        while True:
            message.get_message(message_socket.recv_string())
            command = self.commands[message.message_name]
            command(message, message_socket)
