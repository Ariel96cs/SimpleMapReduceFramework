import zmq
import zmq.asyncio as zmqa
import os
import asyncio
# from multiprocessing import# lock, Process
import sqlite3 as sql
from threading import Thread
from multiprocessing import Process
from message_tools import Message
import message_tools as mt
import sys
import time
import pickle
import timeout_decorator
loop = asyncio.get_event_loop()


class FileSystem:

    def __init__(self, ip="0.0.0.0", port="8080",addr_recv_broadcast=('0.0.0.0',8081)):
        """
        Initializes the needed values for the FileSystem and starts it's server

        :param ip:
            The IP where the FileSystem will run
        :param port:
            The port where the FileSystem will run
        """
        self.names = {
            'PUTDATA': self.try_to_put_data,
            'GETDATA': self.try_to_get_data,
            'PUTJOB': self.try_to_put_job,
            'REMOVEJOB': self.remove_job,
            'INSERTBLOCK': self.try_to_insert_block,
            'UPDATEROW': self.try_to_update_row,
            'BLOCKRESULT': self.try_to_insert_blockresult,
            'INSERTJOB': self.try_to_insert_job,
            'GETROWS': self.try_to_get_rows,
            'APPENDBLOCK': self.try_to_append_block
        }

        self.addr = (ip, port)
        self.last_used_port = str(int(self.addr[1]) + 1)
        self.ping_addr = addr_recv_broadcast

        # self.register_on_system()


    @staticmethod
    def _check_file_lock(file_url: str):
        lock_path = file_url + "._lock"
        if not os.path.exists(lock_path):
            print("FS: Creamos el lock")
            lock_file = open(lock_path, "w")
            lock_file.close()
        else:
            print("FS: Something went wrong getting the file lock...")
            return None,'LockFileError'
        print("FS:File_Lock:Returning")
        return lock_path,None

    def _attend_ping(self):
        return mt.recv_ping_broadcast(self.ping_addr,self.addr)

    def try_to_put_data(self,socket,message):
        print("FS: Enter PutData")
        new_port = mt.get_available_random_port(self.addr[0])
        is_a_byte_message = message.payload["byte"]
        filename = message.payload["file_url"]

        lock, err = self._check_file_lock(filename)
        if lock is None:
            print("FS: Error 403, en el putdata")
            socket.send_string(str(Message().set_message("Error",
                                                         {"info": "Someone is using the file. Cannot access.",
                                                          'type_error': err
                                                          })))
            return -1

        print("FS: Creado el lock haremos put_data")
        try:
            if is_a_byte_message:
                # data_message = data_socket.recv()
                file = open(filename, "wb")
            else:
                # data_message = Message().get_message(data_socket.recv_string())
                file = open(filename, "w")
            print("FS:File correctly open")
            m = Message().set_message("OK", {"ip": self.addr[0], "port": new_port})
            socket.send_string(str(m))
            print("FS:OK message sent")
        except zmq.error.ZMQError:
            socket.send_string(str(Message().set_message("ERROR", {"info": "Something went wrong creating the file."})))
            os.remove(lock)
            print("FS: Removemos el lock: ", lock)
            return -1

        new_addr = (self.addr[0],new_port)
        p = Process(target=self.new_put_data,args=(new_addr,file,is_a_byte_message,filename,lock,))
        p.daemon = True
        p.start()

    def new_put_data(self,new_addr,file,is_a_byte_message,filename,lock):
        print("FS: Vamos a ejecutar el putdata de: ",filename)
        data_context = zmq.Context()
        data_socket = data_context.socket(zmq.REP)
        try:
            data_socket.bind("tcp://{}:{}".format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            os.remove(lock)
            return -1
        try:
            while True:
                print("FS: Preparado para recibir data")
                if is_a_byte_message:
                    data_to_write = mt.raw_recv(data_socket.recv,1)
                else:
                    data_to_write = mt.raw_recv(data_socket.recv_string,1)
                data_socket.send_string(str(Message().set_message("OK")))
                print("FS: Recibido el data")
                if data_to_write != '' and data_to_write != b'':
                    file.write(data_to_write)
                    print("FS: Escribimos en el arhivo: ",data_to_write)
                else:
                    file.close()
                    print("FS: Cerramos el archivo")
                    os.remove(lock)
                    print("FS: Removemos el lock: ", lock)
                    data_socket.close()
                    return filename
        except TimeoutError:
            print("FS: Hubo timeout en el putdata")
            file.close()
            os.remove(lock)
            data_socket.close()
            print("FS: Removemos el lock: ", lock)
        assert False, "FS: Por alguna razon estamos fuera del metodo en put data"

    def try_to_put_job(self,socket,message):
        print("Intentemos hacer putjob")
        job_id = message.payload["job_id"]
        # files = message.payload["files"]
        db_url = str(job_id) + "/database.sqlite3"
        # print("files to save:", files)
        try:
            os.mkdir(job_id)
        except FileExistsError:
            print("FS:Ya esa tarea existe, escribiré sobre la carpeta ya creada")

        new_port = mt.get_available_random_port(self.addr[0])
        message_to_send = Message().set_message("OK", {"ip": self.addr[0], "port": new_port})
        socket.send_string(str(message_to_send))
        new_addr = (self.addr[0], new_port)
        p = Process(target=self.new_put_job,args=(new_addr,job_id,db_url,))
        p.daemon = True
        p.start()

    # Vamos a dejar que el put job sea solamente crear las tablas de status
    # que el cliente se encargue de hacer putdata de las restantes cosas despues
    def new_put_job(self,new_addr,job_id,db_url):
        print("Estamos dentro del putjob")
        data_context = zmqa.Context()
        data_socket = data_context.socket(zmq.REP)
        try:
            data_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            return -1

        answer = mt.loop_tool(mt.try_to_recv,data_socket.recv_string,1)
        if answer == -1:
            data_socket.close()
            print("No me respondieron en el put job")
            return -1
        if answer.message_name != "OK":
            data_socket.close()
            print("Hubo algun error en el putjob")
            return -1

        try:
            init_file = open("./" + str(job_id) + "/__init__.py", "x")
            init_file.close()
            db_file = open(db_url, "x")
            db_file.close()

            # Now I'm gonna configure the database
            connection = sql.connect(db_url)
            cursor = connection.cursor()

            cursor.execute('''CREATE TABLE block 
            (block_id text PRIMARY KEY NOT NULL, state text, phase text,worker_ip text,worker_port text)''')

            cursor.execute('''CREATE TABLE slices_url 
            (slice_url text PRIMARY KEY NOT NULL , block_id text,
            FOREIGN KEY (block_id) REFERENCES block (block_id)) ''')

            cursor.execute('''CREATE TABLE result_url 
            (result_url text PRIMARY KEY NOT NULL )''')

            cursor.execute('''CREATE TABLE block_result 
            (block_id text NOT NULL, result_url text NOT NULL, 
            PRIMARY KEY (block_id,result_url) , 
            FOREIGN KEY (block_id) REFERENCES block (block_id) ON DELETE CASCADE ON UPDATE NO ACTION,
            FOREIGN KEY (result_url) REFERENCES result_url (result_url) ON DELETE CASCADE ON UPDATE NO ACTION
            )''')

            cursor.execute('''CREATE TABLE job 
            (job_id text PRIMARY KEY NOT NULL, tracker_ip_ping text, tracker_port_ping text, answer_ip text, answer_port text,
            status_phase text, map_data_url text, result_url text, job_state text, data_type text)''')
            connection.commit()
            cursor.close()
            connection.close()

        except FileExistsError:
            print("FS:Los archivos de inicialización ya existen... no sé cómo")
        data_socket.send_string(str(Message().set_message("OK", {"job_url": "./" + job_id,
                                                                 "database_url": db_url})))
        data_socket.close()
        print("Se hizo putjob")
        return 0

    @staticmethod
    def remove_job(socket, message):
        """
        Elimina del filesystem los archivos correspondientes a una tarea

        :param socket:
            El socket en el que se manda el mensaje "REMOVEJOB"
        :param message:
            El mensaje "REMOVEJOB" aún por ser interpretado
        :return:
            Nothing
        """
        try:
            for file in os.listdir("./" + message.payload["job_url"] + "/"):
                os.remove("./" + message.payload["job_url"] + "/" + file)
            os.rmdir("./" + message.payload["job_url"] + "/")
            socket.send_string(str(Message().set_message("OK", {"info": "Operation successfully done."})))
            return 0
        except FileNotFoundError:
            socket.send_string(str(Message().set_message("ERROR", {"info": "404: Job not found"})))
            return -1

    def try_to_get_data(self,socket,message):
        print("FS: Tratamos de hacer getdata")
        new_port = mt.get_available_random_port(self.addr[0])
        file_url = message.payload["file_url"]

        if not os.path.exists(file_url):
            print("FS: No existe: ",file_url)
            message_to_send = Message().set_message("ERROR", {"info": "File does not exist: {}. "
                                                                "I can't access this file right now.".format(file_url),
                                                                'type_error': 'FileNotFound'})
            socket.send_string(str(message_to_send))
            # assert False,"este fue el mensaje que me habian mandado: {}".format(str(message))
            return -1

        print("FS: Revisemos que nadie mas lo esta usando")
        lock, err = self._check_file_lock(message.payload["file_url"])
        if lock is None:
            print("FS: Error 403.1: Temporarily Forbidden. I can't access this file right now.")
            message_to_send = Message().set_message("ERROR", {"info": "Temporarily Forbidden. "
                                                          "I can't access this file right now.",
                                                  'type_error': err})
            socket.send_string(str(message_to_send))
            return -1
        try:
            if message.payload["byte"]:
                data = open("./" + message.payload["file_url"], "rb")
            else:
                data = open("./" + message.payload["file_url"], "r")
            message_to_send = Message().set_message("OK", {"ip": str(self.addr[0]), "port": new_port})
            socket.send_string(str(message_to_send))
        except FileNotFoundError:
            message_to_send = Message().set_message("ERROR", {"info": "File is not in the filesystem}",
                                                              "type_error":'FileNotFound'})
            socket.send_string(str(message_to_send))
            # data.close()
            os.remove(lock)
            print("FS: Removemos el lock: ", lock)
            # assert False, "este fue el mensaje que me habian mandado: {}".format(str(message))
            return -1

        new_addr = (self.addr[0],new_port)
        p = Process(target=self.new_get_data,args=(new_addr,data,lock,message,))
        p.daemon = True
        p.start()

    def new_get_data(self, new_addr,data,lock,message):
        print("FS: Vamos a hacer un Getdata")
        data_context = zmq.Context()
        data_socket = data_context.socket(zmq.REP)
        try:
            data_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            os.remove(lock)
            return -1
        try:
            m = mt.recv_message(data_socket.recv_string,1)
        except TimeoutError:
                print("Timed Out! en el get data")
                data.close()
                os.remove(lock)
                print("FS: Removemos el lock: ",lock)
                data_socket.close()
                return 0
        # print("FS: GETDATA received",m)
        if m.message_name != "OK":
            print("FS:Something went Wrong...")
            data.close()
            os.remove(lock)
            print("FS: Removemos el lock")
            data_socket.close()
            return -1
        print("FS: Recibimos OK")
        while True:
            print("FS: Tratamos de mandarle una linea")
            line_to_send = data.readline()
            if message.payload['byte']:
                data_socket.send(line_to_send)
            else:
                data_socket.send_string(line_to_send)
            print("FS: Mandamos la linea: ",line_to_send)
            # print(received)
            if line_to_send == "" or line_to_send == b'':
                print("Se termino el archivo que tiene como lock: ",lock)
                break
            try:
                received = mt.recv_message(data_socket.recv_string,1)

            except TimeoutError:
                print("Timeout!! en el getdata")
                data.close()
                os.remove(lock)
                print("FS: Removemos el lock: ", lock)
                data_socket.close()
                return 0
            if received.message_name != "OK":
                print("FS: Retry, file not correctly sent")

                break
            print("FS: La linea fue recibida por el otro")

        data.close()
        os.remove(lock)
        print("FS: Removemos el lock: ", lock)
        print("FS: Enviado todo el dato")
        data_socket.close()
        return 0

    def try_to_append_block(self, socket, message):
        new_port = mt.get_available_random_port(self.addr[0])
        new_addr = (self.addr[0],new_port)
        socket.send_string(str(Message().set_message("OK",
                                                     {"ip": self.addr[0],
                                                      "port": new_port})))
        p = Process(target=self.append_block,args=(message, new_addr,))
        p.daemon = True
        p.start()

    def append_block(self, message, new_addr):
        context = zmq.Context()
        append_socket = context.socket(zmq.REP)
        try:
            append_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS: ZMQ error!!!")
            return -1
        try:
            answer = mt.recv_message(append_socket.recv_string, 2)
        except TimeoutError or MemoryError:
            print("FS: Timeout error en el appendblock!!!")
            append_socket.close()
            return -1
        try:
            block_size = message.payload['block_size']
            status_db_url = message.payload['status_db_url']
            block_id = message.payload['block_id']
        except KeyError:
            print("FS: Parece que hubo algun error en la entrada")
            append_socket.close()
            return -1

        fields_values_done = [('state', mt.slices_states[-1])]
        fields_values_writing = [('state', mt.slices_states[-2])]
        key = ('block_id', block_id)
        table = 'block'

        # le mandamos ACK que me llego el mensaje
        append_socket.send_string(str(mt.Message().set_message('OK')))
        append_socket.close()
        try:
            data_urls = answer.payload['data_urls']
            pairs = answer.payload['pairs']

        except KeyError:
            print("FS: Parece que hubo algun error en la entrada")
            return -1
        # Actualizamos el estado del bloque a WRITING
        self._update_status_fields(fields_values_writing, key, status_db_url, table)

        print("FS: Vamos a hacerle append al bloque: ",block_id)
        # añadimos cada una de las tuplas a sus files correspondientes
        for i in range(block_size):
            try:
                self._append_data(data_urls[i], pairs[i])
            except pickle.PicklingError or EOFError or pickle.UnpicklingError:
                print("FS: Parece que hubo algun problema en el appendata del bloque: ",block_id)
                # Pasamos entonces a SUBMITTED al bloque
                self._update_status_fields([("state",mt.slices_states[1])], key, status_db_url, table)
                return -1

        print("FS: Le hicimos append al bloque: ",block_id)

        # actualizamos el estado del bloque a DONE
        self._update_status_fields(fields_values_done, key, status_db_url, table)
        return 0

    def _append_data(self,file_url,pair):
        lock, err = self._check_file_lock(file_url)
        print("FS: Haciendo appenddata a:", file_url)
        while lock is None:
            time.sleep(0.5)
            lock, err = self._check_file_lock(file_url)
        print("FS: Pusimos el lock a : ",lock)
        if os.path.exists(file_url):
            file = open(file_url, 'rb')
            data = pickle.load(file)

            values = data[1]
            values += pair[1]
            data = (data[0], values)
            print('El data quedo asi: ', data)
            data = pickle.dumps(data)
        else:
            data = pickle.dumps(tuple(pair))

        file = open(file_url, 'wb')

        file.write(data)
        file.close()
        os.remove(lock)
        print("FS: Removemos el lock: ", lock)
        print("FS: Le hicimos append a: ",file_url)
        return 0

    # def try_to_append_data(self,socket,message):
    #
    #     file_url = message.payload["file_url"]
    #     byte = message.payload["byte"]
    #
    #     lock, err = self._check_file_lock(file_url)
    #     if lock is None:
    #         socket.send_string(str(Message().set_message("ERROR", {"info": "Temporarily forbidden file.",
    #                                                                'type_error': err})))
    #         return -1
    #
    #     new_port = mt.get_available_random_port(self.addr[0])
    #     new_addr = (self.addr[0],new_port)
    #     socket.send_string(str(Message().set_message("OK",
    #                                                  {"ip": self.addr[0],
    #                                                   "port": new_port})))
    #
    #     p = Process(target=self.new_append_data,args=(new_addr,file_url,byte,lock,))
    #     p.daemon = True
    #     p.start()
    #
    # def new_append_data(self,new_addr,file_url,byte,lock):
    #     context = zmq.Context()
    #     append_socket = context.socket(zmq.REP)
    #     try:
    #         append_socket.bind('tcp://{}:{}'.format(*new_addr))
    #     except zmq.error.ZMQError:
    #         print("FS: ZMQ error!!!")
    #         os.remove(lock)
    #         print("FS: Removemos el lock: ", lock)
    #         return 0
    #
    #     if os.path.exists(file_url):
    #         if byte:
    #             file = open(file_url, 'rb')
    #             try:
    #                 data = pickle.load(file)
    #             except EOFError:
    #                 assert False, "the file is " + str(file.readlines())
    #             try:
    #                 answer = mt.raw_recv(append_socket.recv,1)
    #             except TimeoutError:
    #                 print("Hubo un timeout!! en el appenddata")
    #                 file.close()
    #                 os.remove(lock)
    #                 print("FS: Removemos el lock: ", lock)
    #                 append_socket.close()
    #                 return 0
    #             print("FS: Quieren que le haga pickle a esto: ",answer)
    #             try:
    #                 loaded_data = pickle.loads(answer)
    #             except pickle.UnpicklingError:
    #                 os.remove(lock)
    #                 print("FS: Removemos el lock")
    #                 file.close()
    #                 print("FS: Error al intentar hacer pickle.loads a: ",answer)
    #                 # append_socket.send_string(str(Message().set_message('ERROR',
    #                 #                                                     {
    #                 #                                                         'info': 'Error while pickling',
    #                 #                                                         'type_error': 'pickle.UnpicklingError'
    #                 #                                                     })))
    #                 append_socket.close()
    #                 return -1
    #             print('Recibimos esto en el appenddata: ', loaded_data)
    #             values = data[1]
    #             values += loaded_data[1]
    #             data = (data[0],values)
    #             print('El data quedo asi: ', data)
    #             data = pickle.dumps(data)
    #         else:
    #             file = open(file_url, 'r')
    #             data = file.readlines()
    #             try:
    #                 answer = mt.raw_recv(append_socket.recv_string,1)
    #             except TimeoutError:
    #                 print("Hubo un timeout!! en el appenddata")
    #                 file.close()
    #                 os.remove(lock)
    #                 print("FS: Removemos el lock: ", lock)
    #                 append_socket.close()
    #                 return 0
    #             print('Recibimos esto en el appenddata: ', answer)
    #             data += answer
    #             print('El data quedo asi: ', data)
    #
    #         file.close()
    #         if byte:
    #             file = open(file_url, 'wb')
    #         else:
    #             file = open(file_url, 'w')
    #         append_socket.send_string(str(Message().set_message('OK')))
    #         print("FS: Guardamos en el appenddata: ",file_url)
    #     else:
    #         print("FS: Creamos el archivo")
    #         if byte:
    #             print('Es byte')
    #             file = open(file_url, 'wb')
    #             try:
    #                 answer = mt.raw_recv(append_socket.recv,1)
    #             except TimeoutError:
    #                 print("Hubo un timeout!! en el appenddata")
    #                 file.close()
    #                 os.remove(lock)
    #                 os.remove(file_url)
    #                 print("FS: Removemos el lock: ", lock)
    #                 append_socket.close()
    #                 return 0
    #             try:
    #                 loaded_data = pickle.loads(answer)
    #             except pickle.UnpicklingError:
    #                 os.remove(lock)
    #                 os.remove(file_url)
    #                 print("FS : Error al hacer pickle load")
    #                 return -1
    #             print('FS: Recibimos esto en el appenddata: ', loaded_data)
    #             data = answer
    #         else:
    #             file = open(file_url, 'w')
    #             print('No es byte')
    #             try:
    #                 answer = mt.raw_recv(append_socket.recv_string,1)
    #             except TimeoutError:
    #                 print("Hubo un timeout!! en el appenddata")
    #                 file.close()
    #                 os.remove(lock)
    #                 os.remove(file_url)
    #                 print("FS: Removemos el lock: ", lock)
    #                 append_socket.close()
    #                 return 0
    #             print('FS: Recibimos esto en el appenddata: ', answer)
    #             data = answer
    #             print('FS: El data quedo asi: ', data)
    #
    #         append_socket.send_string(str(Message().set_message("OK")))
    #         print("FS: Agregamos en el appenddata: ", file_url)
    #     file.write(data)
    #     file.close()
    #     os.remove(lock)
    #     print("FS: Removemos el lock: ", lock)
    #     append_socket.close()
    #     return 0

    def try_to_insert_block(self,socket,message):
        new_port = mt.get_available_random_port(self.addr[0])
        if not os.path.exists(message.payload["status_db_url"]):
            socket.send_string(str(Message().set_message('ERROR', {
                'info': 'unable to open database file',
                'type_error': 'FileNotFoundError'

            })))
        socket.send_string(str(Message().set_message("OK", {"ip": self.addr[0], "port": new_port})))
        new_addr = (self.addr[0],new_port)
        p = Process(target=self.new_insert_block,args=(new_addr,message,))
        p.daemon = True
        p.start()
        print("Tratamos de insertar un bloque")

    def new_insert_block(self,new_addr,message):
        database = sql.connect(message.payload["status_db_url"])
        insert_context = zmq.Context()
        insert_socket = insert_context.socket(zmq.REP)
        try:
            insert_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            return -1
        try:
            insert_message = mt.recv_message(insert_socket.recv_string,1)
        except TimeoutError:
            print('Timeout!! en el insertblock')
            database.close()
            insert_socket.close()
            return 0

        cursor = database.cursor()
        try:
            cursor.execute("INSERT INTO block VALUES ('{}','{}','{}','','')".format(insert_message.payload["block_id"],
                                                                              insert_message.payload["state"],
                                                                              insert_message.payload["phase"]))
            print("Insertamos el bloque: ", insert_message.payload['block_id'])
        except sql.IntegrityError:
            print("Ya existe ese bloque")
        for slice_url in insert_message.payload["slices_id"]:
            try:
                print("Insertamos este slice: ", slice_url)
                insert_command = "INSERT INTO slices_url VALUES ('{}','{}')" \
                    .format(slice_url, insert_message.payload["block_id"])
                cursor.execute(insert_command)
                print('Ejecutamos el comando de insercion de slices: ', insert_command)
            except sql.IntegrityError:
                print("Ya existe esta tupla: ", slice_url)
        database.commit()
        print('Hacemos commit a la insercion')
        insert_socket.send_string(str(Message().set_message("OK")))

        cursor.close()
        database.close()
        insert_socket.close()
        return 0

    def try_to_update_row(self,socket,message):
        print("FS: Vamos a hacer update row")
        new_port = mt.get_available_random_port(self.addr[0])
        if not os.path.exists(message.payload["status_db_url"]):
            socket.send_string(str(Message().set_message('ERROR', {
                'info': 'unable to open database file',
                'type_error': 'FileNotFoundError'

            })))
        socket.send_string(str(Message().set_message("OK", {"ip": self.addr[0], "port": new_port})))
        new_addr = (self.addr[0], new_port)
        p = Process(target=self.new_update_row, args=(new_addr, message,))
        p.daemon = True
        p.start()

    def new_update_row(self,new_addr,message):

        context = zmq.Context()
        update_socket = context.socket(zmq.REP)
        try:
            update_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            return -1

        try:
            update_message = mt.recv_message(update_socket.recv_string,1)
        except TimeoutError:
            print('Timeout!! en el updaterow')
            return 0
        try:
            status_db_url = message.payload["status_db_url"]
            fields_values = update_message.payload["fields_values"]
            key = update_message.payload["key"]
            table = update_message.payload["table"]
        except KeyError:
            print("FS: Hubo KeyError en el update row")

        self._update_status_fields(fields_values, key, status_db_url, table)

        update_socket.send_string(str(Message().set_message("OK")))
        update_socket.close()
        print("FS: Hicimos el update")
        return 0

    @staticmethod
    def _update_status_fields(fields_values, key, status_db_url, table):
        database = sql.connect(status_db_url)
        cursor = database.cursor()
        update_line = ""
        for fv in fields_values:
            if len(update_line) > 0:
                update_line += ", "
            update_line += fv[0] + " = \"" + fv[1] + "\""
        # print("UPDATE_LINE:", update_line)
        cursor.execute('''UPDATE {} SET {} WHERE {} = {}'''
                       .format(table, update_line,
                               key[0], "\"" + key[1] + "\""))
        database.commit()
        cursor.close()
        database.close()

    def try_to_insert_blockresult(self, socket, message):
        new_port = mt.get_available_random_port(self.addr[0])
        if not os.path.exists(message.payload["status_db_url"]):
            socket.send_string(str(Message().set_message('ERROR', {
                'info': 'unable to open database file',
                'type_error': 'FileNotFoundError'

            })))
        socket.send_string(str(Message().set_message("OK", {"ip": self.addr[0], "port": new_port})))
        new_addr = (self.addr[0], new_port)
        p = Process(target=self.new_insert_blockresult, args=(new_addr, message,))
        p.daemon = True
        p.start()

    def new_insert_blockresult(self,new_addr,message):
        database = sql.connect(message.payload["status_db_url"])
        context = zmq.Context()
        block_result_socket = context.socket(zmq.REP)
        block_result_socket.bind("tcp://{}:{}".format(*new_addr))

        try:
            block_result_message = mt.recv_message(block_result_socket.recv_string,1)
        except TimeoutError:
            print('Timeout!! en el insertblockresult')
            database.close()
            block_result_socket.close()
            return 0
        cursor = database.cursor()
        # cursor.execute('''DELETE FROM block_result WHERE result_url = "{}"'''
        #                .format(block_result_message.payload["block_id"]))
        # database.commit()

        for url in block_result_message.payload["result_url"]:
            try:
                cursor.execute('''INSERT INTO block_result VALUES ("{}", "{}")'''
                               .format(block_result_message.payload["block_id"], str(url)))
            except sql.IntegrityError:
                print("Ya existe la tupla: ", ())
        database.commit()
        block_result_socket.send_string(str(Message().set_message("OK")))
        cursor.close()
        database.close()
        block_result_socket.close()
        return 0

    def try_to_insert_job(self,socket,message):
        new_port = mt.get_available_random_port(self.addr[0])
        socket.send_string(str(Message().set_message("OK", {"ip": self.addr[0], "port": new_port})))
        new_addr = (self.addr[0], new_port)
        p = Process(target=self.new_insert_job, args=(new_addr, message,))
        p.daemon = True
        p.start()

    def new_insert_job(self,new_addr,message):
        database = sql.connect(message.payload["status_db_url"])
        context = zmq.Context()
        insert_job_socket = context.socket(zmq.REP)
        try:
            insert_job_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            return -1

        try:
            insert_job_message = mt.recv_message(insert_job_socket.recv_string,1)
        except TimeoutError:
            print('Timeout!! en el insertjob')
            database.close()
            insert_job_socket.close()
            return 0
        if insert_job_message.message_name != "INSERTJOB":
            print("Something went wrong on INSERTJOB!!!")
        d = insert_job_message.payload
        insert_string = "\"" + str(d["job_id"]) + "\", \"" + str(d["tracker_ip_ping"]) + "\", \"" \
                        + str(d["tracker_port_ping"]) + "\", \"" + str(d["answer_ip"]) + "\", \"" \
                        + str(d["answer_port"]) + "\", \"" + str(d["status_phase"]) + "\", \"" \
                        + str(d["map_data_url"]) + "\", \"" + str(d["result_url"]) + "\", \"" \
                        + str(d["job_state"]) + "\", \"" + str(d["data_type"]) + "\""
        cursor = database.cursor()
        # print(insert_string)
        try:
            cursor.execute('''INSERT INTO job VALUES ({})'''.format(insert_string))
            database.commit()
        except sql.IntegrityError:
            print('Ya existe ese job: ', d["job_id"])
        message_to_send = Message().set_message("OK", {})
        insert_job_socket.send_string(str(message_to_send))
        # print("Sent OK")
        cursor.close()
        database.close()
        insert_job_socket.close()
        return 0

    def try_to_get_rows(self,socket,message):
        new_port = mt.get_available_random_port(self.addr[0])
        if not os.path.exists(message.payload["status_db_url"]):
            socket.send_string(str(Message().set_message('ERROR', {
                'info': 'unable to open database file',
                'type_error': 'FileNotFoundError'

            })))
        new_addr = (self.addr[0], new_port)
        try:
            database = sql.connect(message.payload["status_db_url"])
        except sql.OperationalError:
            socket.send_string(str(Message().set_message('ERROR', {
                'info': 'unable to open database file',
                'type_error': "OperationalError"

            })))
            return -1

        socket.send_string(str(Message().set_message("OK", {"ip": self.addr[0], "port": new_port})))
        p = Process(target=self.new_get_rows, args=(new_addr, message,))
        p.daemon = True
        p.start()

    def new_get_rows(self,new_addr,message):
        database = sql.connect(message.payload["status_db_url"])

        context = zmq.Context()
        get_rows_socket = context.socket(zmq.REP)
        try:
            get_rows_socket.bind('tcp://{}:{}'.format(*new_addr))
        except zmq.error.ZMQError:
            print("FS ZMQERROR")
            return -1

        try:
            get_rows_message = mt.recv_message(get_rows_socket.recv_string,1)
        except TimeoutError:
            print('Timeout!! en el getrows')
            database.close()
            get_rows_socket.close()
            return 0
        cursor = database.cursor()
        where_line = ""
        print(get_rows_message)
        for condition in get_rows_message.payload["filters"]:
            if len(where_line) > 0:
                where_line += " AND "
            where_line += "{} = '{}'".format(*condition)
        if len(where_line) > 0:
            select_line = '''SELECT * FROM {} WHERE {}''' \
                .format(get_rows_message.payload["table"], where_line)
            print('Mandamos a ejecutar el comando de sql: ', select_line)
            cursor.execute(select_line)
        else:
            select_line = '''SELECT * FROM {}''' \
                .format(get_rows_message.payload["table"])
            print('Mandamos a ejecutar el comando de sql: ', select_line)
            cursor.execute(select_line)
        print('Preparamos a respuesta al query')
        headers = [elem[0] for elem in cursor.description]
        result = cursor.fetchall()
        to_send = []
        # To_send es una lista de diccionarios donde cada elemento de la lista
        # es un diccionario con los valores de la fila correspondiente
        for item in result:
            to_send.append({headers[i]: item[i] for i in range(len(headers))})
        print('Le mandamos como respuesta del query: ', to_send)
        database.commit()
        get_rows_socket.send_string(str(Message().set_message("OK", {"rows": to_send})))
        cursor.close()
        database.close()
        get_rows_socket.close()
        return 0

    def start_server(self):
        """
        Escucha lo que se manda por addr y en dependencia del mensaje, se ejecuta la función necesaria

        :param addr:
            Dirección que escuchará el worker
        :return:
            Nothing
        """
        context = zmqa.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://{}:{}".format(self.addr[0], self.addr[1]))

        print("FS:Running in {}".format(self.addr))
        attend_ping = Thread(target=self._attend_ping)
        attend_ping.daemon = True
        attend_ping.start()

        while True:
            print("FS: Into the WHILE")
            text = loop.run_until_complete(socket.recv_string())
            print("FS: Server received:", text)
            message = Message().get_message(text)
            function_to_execute = self.names[message.message_name]
            function_to_execute(socket,message)


