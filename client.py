import asyncio
import zmq
import zmq.asyncio as zmqa
from zmq.asyncio import Context as con
from message_tools import Message
import message_tools as mt
from data_handler import DataHandler
from multiprocessing import Process


class Client:
    def __init__(self, data="./data",
                 functions_file="./function.py",
                 data_type='text',
                 client_ip='0.0.0.0',
                 workers=[]):
        """
        The initializer of the Client instance.

        :param data:
            The url of the file where the data to be processed is stored on the client
        :param functions_file:
            The url of the file where the file (*.py) with the map and reduce functions is stored on the client
        :param data_type:
            The type of the data stored on the data file, might be 'text' or 'table'.
            Is 'text' in the case of needing to be interpreted as plain text
            Is 'table' if the data is well organized to be interpreted as a table
        :param timeout:
            The timeout for the client to wait for an answer from the service manager
        """
        self.tries = 5

        self.functions_file = functions_file
        self.data_type = data_type
        self.job_url = None
        self.data = data
        self.master_ping_addr = tuple()
        self.master_ping_context = zmq.Context()
        self.master_ping_socket = None
        self.client_ip = client_ip
        self.listener_addr = ("0.0.0.0", "7999")
        self.filesystem_addr = None
        self.filesystem_addrs = []
        self.status_handler = None

        self.masters_list = workers
        self.current_master = None
        self._get_new_master()

        self.files_urls = {}
        self.jhash = None
        # Si solamente recibimos str como data
        extra = self._get_extra_info_from_job(data, functions_file)
        self.job_info_for_register = extra
        self.listener_addr = (self.client_ip, mt.get_available_random_port(self.client_ip))

    @staticmethod
    def _get_extra_info_from_job(data, functions_file):
        extra = ''
        import hashlib
        with open(functions_file) as file:
            for i in range(5):
                extra += hashlib.sha1(file.readline().encode()).hexdigest()
        extra = hashlib.sha1(extra.encode()).hexdigest()
        with open(data) as file:
            for i in range(5):
                extra += hashlib.sha1(file.readline().encode()).hexdigest()
        extra = hashlib.sha1(extra.encode()).hexdigest()
        return extra

    def new_put_job(self):
        context = zmqa.Context()
        # new_addr = (self.client_ip,mt.get_available_random_port(self.client_ip))
        filesystem_socket = context.socket(zmq.REQ)
        print("Client: ",'Tratemos de hacer putjob a: ', self.filesystem_addr)
        filesystem_socket.connect("tcp://{}:{}".format(*self.filesystem_addr))
        message = Message().set_message("PUTJOB", {"job_id": self.jhash})
        filesystem_socket.send_string(str(message))
        answer = mt.loop_tool(mt.try_to_recv,filesystem_socket.recv_string)
        if answer == -1:
            print("PUTJOB answer timed out!")
            filesystem_socket.close()
            return -1
        if answer.message_name != 'OK':
            print("No me devolvieron OK")
            filesystem_socket.close()
            return -1
        filesystem_socket.close()
        data_addr = answer.payload["ip"] + ":" + answer.payload["port"]
        putjob_context = zmqa.Context()
        putjob_socket = putjob_context.socket(zmq.REQ)
        putjob_socket.connect("tcp://" + data_addr)
        putjob_socket.send_string(str(Message().set_message("OK")))

        answer = mt.loop_tool(mt.try_to_recv,putjob_socket.recv_string)
        if answer == -1:
            print("PUTJOB answer timed out!")
            putjob_socket.close()
            return -1
        if answer.message_name != "OK":
            print("Error")
            putjob_socket.close()
            return -1
        if 'job_url' in answer.payload and 'database_url' in answer.payload:
            job_url = answer.payload['job_url']
            database_url = answer.payload['database_url']
            putjob_socket.close()
            return job_url,database_url
        putjob_socket.close()
        return -1

    def new_put_data(self,current_file_url,file_url,is_byte):
        context = zmqa.Context()
        # print("Client: FS addr:", self.filesystem_addr)
        filesystem_socket = context.socket(zmq.REQ)
        filesystem_socket.connect("tcp://" + self.filesystem_addr[0] + ":" + self.filesystem_addr[1])
        # print("Client: connected to filesystem")
        message = Message().set_message("PUTDATA", {'file_url':file_url,
                                                    'byte': is_byte
                                                   })
        filesystem_socket.send_string(str(message))
        answer = mt.loop_tool(mt.try_to_recv,filesystem_socket.recv_string)
        if answer == -1:
            print("Client: Putdata answer timed out!")
            filesystem_socket.close()
            return -1
        if answer.message_name != 'OK':
            print("Client: No me devolvieron OK en el put data")
            filesystem_socket.close()
            return 1
        filesystem_socket.close()
        data_addr = answer.payload["ip"] + ":" + answer.payload["port"]
        putdata_context = zmqa.Context()
        putdata_socket = putdata_context.socket(zmq.REQ)
        # print("Client: data addr:", data_addr)
        putdata_socket.connect("tcp://" + data_addr)
        # print("Client:putdata socket connected to " + data_addr)
        # en este momento mandamos el data
        try:
            with open(current_file_url, 'r') as file:
                lines,eof = self._read_some_lines(file,is_byte)
                while True:
                    putdata_socket.send_string(lines)
                    # print("Client: Mandamos el texto: ",lines)
                    if eof:
                        break

                    # print("about to receive the data answer")
                    answer = mt.loop_tool(mt.try_to_recv,putdata_socket.recv_string)
                    if answer.message_name != "OK":
                        print("Client: Data socket answer timed out. Something wrong sending the functions file")
                        return 1
                    if answer == -1:
                        print("Client: Data Message timed out")
                        putdata_socket.close()
                        return -1
                    lines,eof = self._read_some_lines(file,is_byte)
            # data_socket.recv_string()
            putdata_socket.close()
            print("Data sendend: ",current_file_url)
        except FileNotFoundError:
            print('No existe el file: ',current_file_url)
            return 1


    @staticmethod
    def _read_some_lines(file_descriptor, is_byte=False, len_limit=500):
        eof_cha = Client._get_end_character(is_byte)
        lines = eof_cha
        line = file_descriptor.readline()
        while line != eof_cha and len(lines) < len_limit:
            lines += line
            line = file_descriptor.readline()
        if len(lines) >= len_limit:
            lines += line

        eof = lines == eof_cha
        return lines,eof

    @staticmethod
    def _get_end_character(is_byte):
        if is_byte:
            eof_cha = b''
        else:
            eof_cha = ''
        return eof_cha

    def send_job_data_to_fs(self,function_path,data_path,data_is_byte=False):
        put_job_result = self.new_put_job()
        if put_job_result == -1:
            print("No se salvo bien el put job")
            return -1
        job_url, database_url = put_job_result
        function_url = self.jhash + "/functions.py"
        data_url = self.jhash + "/map_data"
        if self.new_put_data(function_path,function_url,False) == -1:
            print("Fallo al enviar las funciones")
            return -1
        if self.new_put_data(data_path,data_url,data_is_byte) == -1:
            print("Fallo al enviar los datos")
            return -1
        self.job_url = job_url

        self.files_urls = {'functions_url': function_url,
                           'data_url': data_url,
                           'db_url': database_url}
        return 0

    def _try_execute_task(self):
        """
        Tries to excecute the task, making ping first. It stablishes the connection

        :param service_addr:
            The address of the service master
        :return:
            The result of the task if completed
        """
        print("Client:on the try...")
        r = self.send_job(self.data, self.functions_file)
        while r == -1:
            self._get_new_master()
            r = self.send_job(self.data, self.functions_file)
        return r

    def send_job(self, data_file, function_file):
        """
        A function to put a job's data on the service's filesystem

        :param data_file:
            The url of the data file to send
        :param function_file:
            The url of the function file to send
        :param data_type:
            The type of data stored on the data file
        :return:
            0 if worked OK
        """
        print("Client: ",'Tratamos de hacer jobreg a: ',self.current_master)
        client_context = zmqa.Context()
        register_socket = client_context.socket(zmq.REQ)
        register_socket.connect("tcp://" + self.current_master[0] + ":" + self.current_master[1])

        register_message = Message().set_message("JOBREG", {"files": [function_file, data_file],
                                                            "extra_info": self.job_info_for_register})
        register_socket.send_string(str(register_message))
        answer = mt.loop_tool(mt.try_to_recv,register_socket.recv_string)
        if answer == -1:
            print("Client: ",'No me respondio: ',self.current_master)
            register_socket.close()
            return -1
        print("Client: ", "Recibi del jobreg: ",answer)
        # print("Client:answer del JOBREG:", answer)
        if answer.message_name == "OK":
            self.jhash = answer.payload["job_id"]
            self.masters_list = answer.payload["trackers_addr"]
            # print(answer.payload["filesystem"])
            self.filesystem_addrs = answer.payload["filesystem"]
            self.filesystem_addr = self._get_new_filesystem_node()

            if "info" in answer.payload.keys():
                print("Info:", answer.payload["info"])
        else:
            print("Client:Job not correctly sent")
            return -1

        # First sends the files to the filesystem

        while True:
            r = self.send_job_data_to_fs(function_file, data_file)
            if r == -1:
                print("Client: Problem with the FS")
            #     todo: tengo que escoger otro nodo de filesystem
                self.filesystem_addr = self._get_new_filesystem_node()
            else:
                break
        register_socket.close()
        assert self.try_to_connect_master() != -1, "No hay workers disponibles"

        return 0

    def _get_new_master(self):
        assert len(self.masters_list) != 0, "Client: No workers availables"
        self.current_master = self.masters_list.pop()

    def _get_new_filesystem_node(self):
        import random
        assert len(self.filesystem_addrs) != 0, "No existen nodos del filesystem"
        r = random.randrange(0,len(self.filesystem_addrs))
        return self.filesystem_addrs.pop(r)

    def show_progress_job(self):
        from progress_job_iterator import ProgressTaskIterator
        from status_comands import StatusHandler
        from tqdm import tqdm

        status_db_url = self.files_urls["db_url"]
        self.status_handler = StatusHandler(status_db_url,self._verify_if_errors_in_fs,
                                            self._reset_method_if_no_answer_from_fs)
        map_result_iterator = ProgressTaskIterator(self.jhash,'map',self.status_handler,self.filesystem_addr)
        map_done_blocks = [block_id for block_id in tqdm(map_result_iterator,desc="Map Progress")]

        reduce_result_iterator = ProgressTaskIterator(self.jhash, 'reduce', self.status_handler, self.filesystem_addr)
        reduce_done_blocks = [block_id for block_id in tqdm(reduce_result_iterator, desc="Reduce Progress")]
        print("Client: DONE JOB!!!")

    def execute(self):
        context = zmqa.Context()
        socket = context.socket(zmq.REP)
        print("Client:Binded to the listener port:", "tcp://" + self.listener_addr[0] + ":" + self.listener_addr[1])
        result = self._try_execute_task()
        socket.bind("tcp://" + self.listener_addr[0] + ":" + self.listener_addr[1])
        assert result != -1, "No se pudo ejecutar el el job"
        # print("client: result:", result)
        p = Process(target=self.show_progress_job)
        p.daemon = True
        p.start()
        print("Client: Cargando....")
        while True:  # Está en un while True para que se pueda mandar información por el socket ademas del resultado

            response = mt.loop_tool(mt.try_to_recv, socket.recv_string,5)
            if response == -1:
                # print("Client: Result response timed out!")

                if self._ping_master():
                    # print("Client: ",'Aun esta corriendo el master')
                    continue
                else:
                    print("Client: Se cayo el master anterior,busquemos otro")
                    self.try_to_connect_master()
                continue

            # Le respondo y después reviso lo que me mandó, si de todas formas le voy a responder lo mismo
            socket.send_string(str(Message().set_message("OK")))
            if response.message_name == "DONE" or response.message_name == "ERROR":
                if response.message_name == "ERROR":
                    print("Client:Error occurred during the operation: ", response.payload["info"])
                    return -1
                print("Listo el resultado")
                break
            if response.message_name == "RELOCATE":
                print("Client: ","Me Mandaron a hacer Relocate")
                self.master_ping_addr = (response.payload["ping_tracker_ip"], response.payload["ping_tracker_port"])
                self.listener_addr = (response.payload["answer_ip"], response.payload["answer_port"])
                socket.close()
                socket = context.socket(zmq.REP)
                socket.bind("tcp://"+self.listener_addr[0]+":"+self.listener_addr[1])
                continue
            else:
                print("Client:Operation info:", response.payload["info"])
        socket.close()
        result_url = response.payload["result_url"]
        print("Client:The result is in:", result_url, ", inside the filesystem")
        current_tries = 0
        result_data = None
        # Tratamos de recoger los resultados
        while current_tries < self.tries:
            result_data = self.new_get_data(result_url)
            if result_data == -1:
                print("Hubo bateo al buscar los resultados")
                self.filesystem_addr = self._get_new_filesystem_node()
                if self.filesystem_addr == -1:
                    print("Client: No pudimos resolver los resultados")
                    return -1
            else:
                break

        # self.remove_job()
        return result_data

    def remove_job(self):
        current_tries = 0
        # Tratamos de remover e job
        print("Client: ",'Tratamos de remover el job')
        while current_tries < self.tries:
            removed = self._remove_job()
            if removed == -1:
                self.filesystem_addr = self._get_new_filesystem_node()
                if self.filesystem_addr == -1:
                    print("Client: No pudimos remover el job")
                    return -1
            else:
                break

    def _remove_job(self):
        sock = zmqa.Context().socket(zmq.REQ)
        sock.connect("tcp://{}:{}".format(*self.filesystem_addr))
        sock.send_string(str(Message().set_message("REMOVEJOB",{'job_url':self.job_url})))
        answer = mt.loop_tool(mt.try_to_recv,sock.recv_string)
        if answer == -1:
            print("Client: No me respondio el FileSystem")
            return -1
        if answer.message_name == "OK":
            print("Client: Job removed")
        else:
            print("Client: error al remover el job:",answer.payload['info'])
        return 0

    def new_get_data(self,file_url,is_byte_data=True):
        context = zmqa.Context()
        filesystem_socket = context.socket(zmq.REQ)
        # print("Vamos a buscar los resultados")
        print("Client:filesystem addr:", "tcp://" + self.filesystem_addr[0] + ":" + self.filesystem_addr[1])
        filesystem_socket.connect("tcp://{}:{}".format(*self.filesystem_addr))
        filesystem_socket.send_string(str(Message().set_message("GETDATA", {"byte": is_byte_data, "file_url": file_url})))

        data_sock_message = mt.loop_tool(mt.try_to_recv,filesystem_socket.recv_string)
        if data_sock_message == -1:
            print("No me respondio el filesystem")
            return -1

        if data_sock_message.message_name != "OK":
            print("Client:Some error happened getting the message with the ip and port to get the result of "
                  "the operation")
            print("instead, data sock message was:", str(data_sock_message))
            return 1

        data_socket = context.socket(zmq.REQ)
        data_socket.connect("tcp://{}:{}".format(data_sock_message.payload["ip"],data_sock_message.payload["port"]))
        data_socket.send_string(str(Message().set_message("OK")))
        # print("Nos preparamos para recibir linea por linea")
        eof_cha = Client._get_end_character(is_byte_data)

        result_byte = eof_cha

        temp_line = mt.loop_tool(mt.raw_try_to_recv,data_socket.recv)
        if temp_line == -1:
            print("No me respondio el filesystem")
            return -1

        while temp_line != eof_cha:
            # print("Recibimos la linea")
            data_socket.send_string(str(Message().set_message("OK")))
            result_byte += temp_line
            temp_line = mt.loop_tool(mt.raw_try_to_recv,data_socket.recv)
            if temp_line == -1:
                print("No me respondio el filesystem")
                return -1

        import pickle
        result_data = pickle.loads(result_byte)
        # print("Tenemos los resultados: ", result_data)
        return result_data


        # return 0

    def _ping_master(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://{}:{}".format(*self.master_ping_addr))
        message = Message().set_message("PING", {"info": "Are you there?"})

        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message,message,self.master_ping_addr,
                                                     lambda: print("Client: ","No me respondio el master"),2)
        if answer is None:
            return False
        return True

    def try_to_connect_master(self):

        print("Client: ",'Esperando respuesta por: ',self.listener_addr)
        while True:
            print("Client: Into the while trying to get some master")
            sock = zmqa.Context().socket(zmq.REQ)
            self._get_new_master()
            print("Client: ","Trying to cannect to: ",self.current_master)
            sock.connect("tcp://{}:{}".format(*self.current_master))
            sock.send_string(str(Message().set_message("JOB", {"job_url": self.job_url,
                                                               "job_id": self.jhash,
                                                               "data_type": self.data_type,
                                                               "client_addr": self.listener_addr,
                                                               "functions_url": self.files_urls["functions_url"],
                                                               "map_data_url": self.files_urls["data_url"],
                                                               "status_db_url": self.files_urls["db_url"]
                                                               })))

            print("Enviado el mensaje de job")
            answer = mt.loop_tool(mt.try_to_recv,sock.recv_string)
            if answer == -1:
                print("Client: ", self.current_master, " did not responded")
                self._get_new_master()
                continue

            print("Client:JOB answer:", answer)

            if answer.message_name == "OK":
                self.master_ping_addr = (answer.payload["ping_tracker_ip"], answer.payload["ping_tracker_port"])
                print("Client: ","Sended job")
                return 0

    def _verify_if_errors_in_fs(self, answer, check_for_error=True):
        if answer is None:
            # payload = {
            #     'info': "File System didn't answer",
            #     'type_error': 'FileSysError',
            #     'send_to_client': True
            # }
            # self.send_error_message_to_addr(payload,self.client_addr)
            print("Client: ","El filesystem no me respondio")
            return -1
        elif isinstance(answer,mt.Message) and check_for_error and answer.message_name == 'ERROR':
            error_info = answer.payload['info']
            if 'type_error' in answer.payload.keys():
                type_error = answer.payload['type_error']
                print('FileSystemError: ',error_info)
                print('TypeError: ',type_error)
                if type_error == 'LockFileError':
                    return 1
            return -2

    def _reset_method_if_no_answer_from_fs(self,f,*args):
        print("Client: ",'Vamos a resetear la tarea: ',f)
        if len(self.filesystem_addrs) == 0:
            self._get_new_filesystem_node()

        self._get_new_filesystem_node()
        print("JobTracker: ", 'Reseteamos la tarea: ',f)
        return f(self.filesystem_addr,*args[1:])

