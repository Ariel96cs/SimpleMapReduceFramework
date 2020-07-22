import importlib
import message_tools as mt
import os
from status_comands import StatusHandler
from data_handler import DataHandler
from sys import exit
import hashlib
from threading import Thread


class TaskTracker:
    def __init__(self,file_sys_broadcast_addr,task_ping_addr,
                 current_worker_addr,
                 job_url,
                 job_id,
                 function_url,
                 block_urls,
                 task,
                 answer_addr,
                 load_byte,
                 status_db_url,block_id):

        self.status_handler = StatusHandler(status_db_url,
                                            self._verify_if_errors_in_fs,
                                            self._reset_method_if_no_answer_from_fs)
        self.data_handler = DataHandler(job_url,
                                        job_id,
                                        self._verify_if_errors_in_fs,
                                        self._reset_method_if_no_answer_from_fs)
        self.task_ping_addr = task_ping_addr
        self.file_sys_broadcast_addr = file_sys_broadcast_addr
        self.file_sys_addrs = []
        self.answer_addr = answer_addr
        self._update_filesystem_nodes()
        self.file_sys_addr = self._get_new_filesystem_node()
        self.job_url = job_url
        self.job_id = job_id
        self.function_url = function_url
        self.block_urls = block_urls
        # print("Task_Exc: Este es el block_urls: ",self.block_urls)
        self.task = task
        self.load_byte = load_byte
        self.current_worker_addr = current_worker_addr
        self.status_db_url = status_db_url
        self.block_id = block_id

        self.map_fun, self.red_fun, self.comb = self.get_func()
        self.record_readers = {
            True: self.record_reader_byte,
            False: self.record_reader_str
        }
        self.execute_methods = {
            'map': self.execute_map_task,
            'reduce': self.execute_reduce_task
        }

        self.start_listen_pings()
        #  nos aseguramos que si se cayo el master en el momento que me mando el mensaje de task, yo mismo pongo en
        # submitted el bloque
        to_update = [('state', mt.slices_states[1]), ('worker_ip', self.task_ping_addr[0]),
                     ('worker_port', self.task_ping_addr[1])]
        print("Task_Exc: ", "Salvamos el estado del bloque {} a SUBMITTED en el filesys: {}"
              .format(block_id, self.file_sys_addr))

        self.status_handler.update_status_row(self.file_sys_addr, 'block', ('block_id', block_id), to_update)

        self.execute_task = self.execute_methods[self.task]
        self.record_reader = self.record_readers[load_byte]

    def _answering_pings(self):
        import zmq
        context = zmq.Context()
        sock = context.socket(zmq.REP)
        sock.bind('tcp://{}:{}'.format(*self.task_ping_addr))

        while True:
            message = mt.Message().get_message(sock.recv_string())
            print('Task_Exc: ', ' me llego este ping: ',message)
            sock.send_string(str(mt.Message().set_message("OK")))

    def start_listen_pings(self):
        p = Thread(target=self._answering_pings)
        p.daemon = True
        p.start()

    def get_func(self):
        print('Task_Exc: ','this is my current working Directory ',os.getcwd())
        print("Task_Exc: ","Vamos a buscar los datos de las funciones")
        function_data = self.data_handler.get_str_data(self.file_sys_addr,data_url=self.function_url)[0]
        print(function_data)
        with open('./mr_modules/{}_function.py'.format(self.job_id), 'w') as file:
            file.write(function_data)
            print('Task_Exc: ',"Escribimos sobre el archivo function")
        try:
            fun = importlib.import_module('mr_modules.{}_function'.format(self.job_id), 'modules.subpkg')
            importlib.invalidate_caches()
            mapper = fun.mapper
            reducer = fun.reducer
            combiner = fun.combiner
        except AttributeError:
            print("Task_Exc: ","Can't import functions")
            payload = {
                'info': "Can't import function",
                'type_error': 'FunctionError',
                'send_to_client': True
            }
            self.send_error_message_to_addr(payload,self.answer_addr)

        return mapper, reducer, combiner

    def record_reader_byte(self, slice_url):
        # print('Task_Exc: ', "Vamos a buscar el slice url: ",slice_url)
        return self.job_url, self.data_handler.get_pyobj_data(self.file_sys_addr,data_url=slice_url)[0]

    def record_reader_str(self, slice_url):
        # print('Task_Exc: ', "Vamos a buscar el slice url: ", slice_url)
        return self.job_url, self.data_handler.get_str_data(self.file_sys_addr,data_url=slice_url)[0]

    # tengo que cambiar la forma de agrupar por las llaves
    # para guardar los todos los resultados del map y luego agruparlos
    # para pasarlos al combiner.
    def execute_map_task(self):
        print('Task_Exc: ',"Voy a ejecutar a map task")
        map_results = []
        for slice_url in self.block_urls:
            input_data = self.record_reader(slice_url)

            try:
                # el map devuelve una lista de (key,value)
                map_results += self.map_fun(*input_data)
            except:
                print('Task_Exc: ',"No pudo hacer el map")
                payload = {
                    'info': "Error while mapping",
                    'type_error': 'OperationError',
                    'send_to_client': True
                }
                self.send_error_message_to_addr(payload,self.answer_addr)
        # esto es otra lista de tuplas (key,value)
        combiner_results = self.combiner(map_results)
        slices_result_urls,pairs = self.get_results_urls(combiner_results,'map_result_')

        res = self.status_handler.check_if_block_is_done(self.file_sys_addr,self.block_urls[0])
        if res == -1:
            print("Task_Exc: ",'FileNotFoundError from fs')
            self.send_done_to_current_worker(self.current_worker_addr, 'task')
        else:
            was_already_done,block_id = res
            if not was_already_done:
                print("Task_Exc: ",' El bloque: ',block_id, ' no estaba en done')
                self.save_map_results(slices_result_urls,pairs,block_id)
                # self.status_handler.put_done_block_in_db(self.file_sys_addr, slices_result_urls, self.block_urls)
                self.status_handler.insert_block_result(self.file_sys_addr,block_id,slices_result_urls)
            else:
                print("Task_Exc: ", ' ->>>>>>>>>>>>>>>>>>>El bloque: ', block_id, ' estaba en done')
            self.send_finished_task()

    def send_error_message_to_addr(self, payload,addr):
        error_message = mt.Message().set_message('ERROR', payload)
        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, error_message, addr,
                              lambda: print('Task_Exc: ', 'No me respondio el tracker'))
        if answer is not None:
            print('Task_Exc: ', 'Me respondio el tracker')

        self.send_done_to_current_worker(self.current_worker_addr, 'task')

    def save_reduce_results(self, data_url,data):
        print("Task_Exc: ", 'Tratemos de salvar los resultados del reduce')
        self.data_handler.save_split_byte_data(self.file_sys_addr,data_url, data)
        return data_url

    def get_reduce_result_url(self):
        split_name = ''
        print("Task_Exc: ", 'Tratemos de salvar los resultados del reduce')
        for x in self.block_urls:
            split_name += x
        split_name = hashlib.sha1(split_name.encode()).hexdigest()
        data_url = '{}/reduce_result_{}'.format(self.job_url, split_name)
        return data_url

    def send_finished_task(self):
        payload = {
            'worker_addr': self.current_worker_addr,
            'block_id': self.block_id
        }
        print('Task_Exc: ','Voy a mandarle Done al tracker por: ',self.answer_addr)
        message = mt.Message().set_message("DONE", payload)
        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, self.answer_addr,
                              lambda: print('Task_Exc: ','No pude mandarle Done al tracker'))

        print('Task_Exc: ', 'Esta fue la respuesta del JobTracker ',answer)
        if answer is None:
            print('Task_Exc: ',"No me respondieron")

        self.send_done_to_current_worker(self.current_worker_addr,'task')

    def send_done_to_current_worker(self,current_worker_addr,role):
        # Le mandamos a mi propio worker que terminamos la tarea
        print('Task_Exc: ', 'Mandamos DONE al propio worker')
        message = mt.Message().set_message('DONE', {'role': role})
        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message,
                              current_worker_addr,
                              lambda: print('Task_Exc: ', 'No me respondio mi propio worker'))

        if answer is not None:
            print('Task_Exc: ','Termine')
        exit()

    def combiner(self,data):
        print("Task_Exc: ","Ejecutamos el combiner")
        result = []
        # print('Task_Exc: ','estos son los datos que me pasaron del map: ',data)
        data = self._group_by_key(data)

        try:
            for key_value_pair in data:
                result.append(self.comb(*key_value_pair))
        except:
            payload = {
                'info': "Error while combining",
                'type_error': 'OperationError',
                'send_to_client': True
            }
            self.send_error_message_to_addr(payload,self.answer_addr)
        # print('Task_Exc: ', 'Este es el resultado del combiner: ', result)
        return result

    def execute_reduce_task(self):
        '''
        El block de url contiene las url de archivos, donde cada uno
        guarda una llave y una lista de valores a los que le haremos reduce.

        :return:
        '''
        print('Task_Exc: ',"voy a ejecutar a reduce task")
        reduce_results = []
        for slice_url in self.block_urls:
            grouped_data = self.record_reader(slice_url)[1]

            # aqui guardaremos los (key,value) que devuelve el reduce
            # print('Task_Exc: '," Asi quedo cuando agrupamos:",grouped_data)

            try:
                # reduce devuelve
                reduce_result = self.red_fun(*grouped_data)
            except:
                print('Task_Exc: ','Error while reducing')
                payload = {
                    'info': "Error while reducing",
                    'type_error': 'OperationError',
                    'send_to_client': True
                }
                self.send_error_message_to_addr(payload,self.answer_addr)
                return 0
            reduce_results.append(reduce_result)
        #     todo: arreglar que es lo que voy a devolver en el reduce
        # print("Task_Exc: ",'Este es el resultado del reduce: ',reduce_results)
        result_url = self.get_reduce_result_url()
        res = self.status_handler.check_if_block_is_done(self.file_sys_addr, self.block_urls[0])
        if res == -1:
            print('Task_Exc: ', 'FileNotFoundError from fs')
            self.send_done_to_current_worker(self.current_worker_addr,'task')
        else:
            was_already_done,block_id = res
            if not was_already_done:
                print("Task_Exc: ","el bloque: ",block_id, ' no estaba en done')
                self.save_reduce_results(result_url,reduce_results)
                self.status_handler.put_done_block_in_db(self.file_sys_addr, [result_url], self.block_urls)
            else:
                print("Task_Exc: ", ' ->>>>>>>>>>>>>>>>>>>El bloque: ', block_id, ' estaba en done')
            self.send_finished_task()

    def _group_by_key(self, pairs):
        # print('Task_Exc: ',"vamos a agrupar estos pares ",pairs)
        temp_dic = {k: [] for k, _ in pairs}
        for k, v in pairs:
            temp_dic[k].append(v)
        return [x for x in temp_dic.items()]

    def save_map_results(self,data_urls,pairs,block_id):
        print("Task_Exec: ","Tratemos de guardar los resultados del map del bloque: ",self.block_id)
        # pairs = list(map(lambda x: (x[0],[x[1]]),pairs))
        self.data_handler.save_block(self.file_sys_addr,data_urls,pairs,self.status_db_url,block_id)
        print('Task_ Exc: ',"Guardados los resultados")
        return 0

    def get_results_urls(self,results,prefix=''):
        data_urls = []
        pairs = []
        for k, v in results:
            split_name = hashlib.sha1(str(k).encode()).hexdigest()
            data_url = '{}/{}{}'.format(self.job_url, prefix,split_name)
            data_urls.append(data_url)
            key_value_pair = (k, [v])
            pairs.append(key_value_pair)
        return data_urls,pairs

    def _verify_if_errors_in_fs(self, answer, check_for_error=True):
        if answer is None:
            # payload = {
            #     'info': "File System didn't answer",
            #     'type_error': 'FileSysError',
            #     'send_to_client': True
            # }
            # self.send_error_message_to_addr(payload,self.answer_addr)
            return -1
        elif isinstance(answer,mt.Message) and check_for_error and answer.message_name == 'ERROR':
            error_info = answer.payload['info']
            if 'type_error' in answer.payload.keys():
                type_error = answer.payload['type_error']
                print("Task_Exc: ",'FileSystemError: ', error_info)
                print("Task_Exc: ",'TypeError: ', type_error)
                if type_error == 'LockFileError':
                    return 1
            self.send_error_message_to_addr(answer.payload,self.answer_addr)

    def _reset_method_if_no_answer_from_fs(self, f, *args):
        if len(self.file_sys_addrs) == 0:
            self._update_filesystem_nodes()

        self.file_sys_addr = self._get_new_filesystem_node()
        return f(self.file_sys_addr, *args[1:])

    def _update_filesystem_nodes(self):
        print("Worker: ", 'Mandamos a buscar los nodos del filesystem')
        filesystem_nodes = mt.send_broadcastmessage(self.file_sys_broadcast_addr)
        print("Worker: ", "Estos son los nodos del filesystem: ", filesystem_nodes)
        self.file_sys_addrs = filesystem_nodes
        if len(filesystem_nodes) == 0:
            print("Worker: ", 'no me respondieron los nodos del filesystem')
            exit()
        return 0

    def _get_new_filesystem_node(self):
        import random
        assert len(self.file_sys_addrs) != 0, "No existen nodos del filesystem"
        r = random.randrange(0,len(self.file_sys_addrs))
        return self.file_sys_addrs.pop(r)