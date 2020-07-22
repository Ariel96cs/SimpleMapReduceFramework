import zmq
from multiprocessing import Process
import pickle
import message_tools as mt
import asyncio
import zmq.asyncio as zmqa
from status_comands import StatusHandler
from data_handler import DataHandler
from sys import exit
from threading import Thread
import time


class JobTracker:

    def __init__(self, worker_broadcast_addr,
                 filesystem_broadcast_addr,
                 tracker_addr_ping,
                 tracker_ip,
                 current_worker_addr,
                 job_url,
                 job_id,
                 data_type,
                 client_addr,
                 functions_url,
                 map_data_url,
                 status_db_url
                 ):

        self.worker_broadcast_addr = worker_broadcast_addr
        self.status_handler = StatusHandler(status_db_url,
                                            self._verify_if_errors_in_fs,
                                            self._reset_method_if_no_answer_from_fs)
        self.data_handler = DataHandler(job_url, job_id, self._verify_if_errors_in_fs,
                                        self._reset_method_if_no_answer_from_fs)

        self.filesystem_broadcast_addr = filesystem_broadcast_addr
        self.job_url = job_url
        self.file_sys_addrs = []
        self._update_filesystem_nodes()
        self.file_sys_addr = self._get_new_filesystem_node()
        self.client_addr = client_addr
        self.job_id = job_id

        self.current_worker_addr = current_worker_addr

        self.data_type = data_type
        self.states = ["map", "reduce"]
        self.job_phase = self.states[0]
        self.veto_workers = []
        self.tracker_ip = tracker_ip
        self.tracker_addr_ping = tracker_addr_ping
        self.tracker_addr = (tracker_ip, '8080')
        self.delimiters = [' ', '\n']
        self.map_results = None
        self.result_data_url = '{}/result_data'.format(self.job_url)
        self.map_data_url = map_data_url
        self.functions_url = functions_url
        self.status_db_url = status_db_url
        self.phases = ['GETWORKERS','SLICES','SENDTASK','WAITANSWERS','GETRESULTS','DONE']
        self.load_job_methods = {
            'GETWORKERS':self.getting_workers,
            'SLICES': self.getting_workers,
            'SENDTASK': self._load_send_task_phase,
            'WAITANSWERS':self._load_wait_results,
            'GETRESULTS':self.getting_results,
        }
        self.status_phase = mt.task_phases[0]

        self.pinging_process = None
        self.get_data = self.data_handler.get_line_by_line_str

    def _load_wait_results(self):
        '''
        En esta etapa los bloques estan como minimo en estado de SUBMITTED y como maximo en DONE, luego
        lo que se va a ejecutar en esta etapa es cargar los bloques que estan en esta fase y que esten en estado
        SUBMITTED, esperar un tiempo fijo, volver a pedirlos, si son la misma cantidad, entonces asumo que los workers que
        se encargaban de esas tareas, ya no estan corriendo, y entonces re assigno las tareas y paso a wait_results
        :return:
        '''
        print("JobTracker: ","Tratamos de cargar la fase de wait for results")
        # import time
        # filters = [('phase',self.job_phase),('state',mt.slices_states[1])]
        # print("JobTracker: ",'Estamos en la fase: ',self.job_phase)
        # blocks = self.status_handler.get_status_rows(self.file_sys_addr,'block',filters)
        # cnt_blocks_submitted = len(blocks)
        # all_blocks_are_done = False
        # while True:
        #     if cnt_blocks_submitted == 0:
        #         all_blocks_are_done = True
        #         break
        #     time.sleep(1)
        #     blocks = self.status_handler.get_status_rows(self.file_sys_addr,'block', filters)
        #     cnt_blocks_2 = len(blocks)
        #     if cnt_blocks_submitted == cnt_blocks_2:
        #         break
        #     elif cnt_blocks_2 < cnt_blocks_submitted:
        #         cnt_blocks_submitted = cnt_blocks_2
        # if not all_blocks_are_done:
        #     blocks,_ = self._get_blocks_urls(filters)
        #     print("JobTracker: ",'En la fase de cargar la etapa de wait results encontr estos bloques sin done: ',blocks)
        #     workers_addr, workers_status = self._getting_workers()
        #     put_byte = self._check_if_data_is_byte()
        #     self.send_tasks(blocks,put_byte,workers_addr,workers_status)
        #     return 0
        # print("JobTracker: ",'Pasamos a la fase de gettingResults')
        # self.getting_results()
        return self._load_send_task_phase()

    def _load_send_task_phase(self):
        '''
        En la fase de send task el status es que tenemos insertado los bloques y estan almenos en estado ADDED
        y a lo sumo en estado SUBMITTED. Lo que haremos será cargar todos los bloques que estan en la fase actual(map o
        reduce), los que estén en ADDED los mandamos con sendtaskmessage, entonces en este punto tenemos todos en
        almenos SUBMITTED y empezamos el ciclo en wait_result.

        :return:
        '''
        print("JobTracker: ",'Tratamos de cargar la fase de sendtask')
        filters = [('phase',self.job_phase)]
        blocks,blocks_status = self._get_blocks_urls(filters)
        print("JobTracker: Tratando de cargar la fase de sendtask queda al cargar los bloques:"
              "------->blocks ",blocks, '---> status ',blocks_status)
        unsubmitted_blocks = [block for i, block in enumerate(blocks)
                              if blocks_status[i]['state'] == mt.slices_states[0] and len(block) > 0]
        print("JobTracker: Unsubmittedblocks: ",unsubmitted_blocks)

        workers_addrs, workers_status = self._getting_workers()
        put_byte = self._check_if_data_is_byte()
        assign_tasks = {block_row['block_id']: (block_row['worker_ip'],block_row['worker_port'])
                        for i,block_row in enumerate(blocks_status) if block_row['worker_ip'] != ''}
        # si quedó algún bloque sin asignar
        if len(unsubmitted_blocks) > 0:
            _,assign_tasks2 = self.distribute_slices(unsubmitted_blocks,workers_status,put_byte,True)
            for x, y in assign_tasks2.items():
                assign_tasks[x] = y
        else:
            r_port = mt.get_available_random_port(self.tracker_ip)
            self.tracker_addr = (self.tracker_ip,r_port)
        # Ahora en este estado todos los bloques estan en estado de SUBMITTED
        print("JobTracker: ","Pasamos a la fase de wait_results")
        self.wait_results(assign_tasks,workers_addrs,put_byte)

    def _check_if_data_is_byte(self):
        return self.job_phase == self.states[1]

    def _answering_pings(self):
        context = zmq.Context()
        sock = context.socket(zmq.REP)
        sock.bind('tcp://{}:{}'.format(*self.tracker_addr_ping))

        while True:
            message = mt.Message().get_message(sock.recv_string())
            print('JobTracker: ', ' me llego este ping: ',message)
            sock.send_string(str(mt.Message().set_message("OK")))

    def _check_if_exists_job(self):
        lines = self.status_handler.get_status_rows(self.file_sys_addr,'job',catch_error=False)
        if len(lines) == 0 or lines == -1:
            self.insert_job()
            return 0
        line = lines[0]

        # si termino el job
        if line['status_phase'] == mt.task_phases[-1]:
            result_url = line['result_url']
            self.send_result(result_url)
        # si no ha terminado el job
        else:
            # primero verificamos que aun este corriendo el tracker de ese job haciendo ping
            tracker_addr = (line['tracker_ip_ping'],line['tracker_port_ping'])
            answer_addr = (line['answer_ip'],line['answer_port'])
            message = mt.Message().set_message('PING')
            tracker_alaive = mt.loop_tool(mt.try_to_send_message,mt.send_message,message,tracker_addr,
                                          lambda :print("JobTracker: ","No me respondio el tracker"))
            # verificamos la respuesta
            if tracker_alaive is not None and isinstance(tracker_alaive,mt.Message) \
                    and tracker_alaive.message_name == "OK":
                # aun esta corriendo el tracker, luego mandamos al cliente el addr
                # por donde le van a mandar la respuesta
                print('JobTracker: ','Mandamos hacer Relocate')
                message = mt.Message().set_message('RELOCATE',{
                    'ping_tracker_ip':tracker_addr[0],
                    "ping_tracker_port":tracker_addr[1],
                    "answer_ip": answer_addr[0],
                    'answer_port': answer_addr[1]
                })
                answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message, self.client_addr,
                                      lambda: print("JobTracker: ", "No me respondio el tracker"))
                print("JobTracker: ",'El cliente me respondio: ',answer)
                self.send_done_to_current_worker(self.current_worker_addr,'job')
            else:
                # el tracker del job no esta corriendo posiblemente pq se desconectó
                print("JobTracker: ",'El tracker de ese job:{} no esta corriendo'.format(self.job_id))

                print('JobTracker: ',"Asumire la tarea")
                # lo próximo es hacerse cargo de la tarea
                # para eso lo primero que debe hacer es actualizar el status y poner su addr de ping
                # todo: recordarle a luiso que la llave de job es un entero, el self.job_id
                new_ping_addr_changes = [('tracker_ip_ping',self.tracker_addr_ping[0]),
                                         ('tracker_port_ping',self.tracker_addr_ping[1])]
                # mandamos a actualizar el tracker que se encargara del job
                self.status_handler.update_status_row(self.file_sys_addr,'job',('job_id',self.job_id), new_ping_addr_changes)
                # actualizamos el estado del job (map o reduce)
                self.job_phase = line['job_state']
                # recordamos el tipo de dato quese estaba analizando
                self.data_type = line['data_type']
                self.status_phase = line['status_phase']
                self.load_job_methods[self.status_phase]()

    def start_listen_pings(self):
        p = Thread(target=self._answering_pings)
        p.daemon = True
        p.start()
        self.pinging_process = p

    def _getting_workers(self):
        '''
        Manda a buscar todos los workers y a cada uno le hacer unr equest por su status

        :return: una tupla (workers_addrs, workers_status) de los workers activos.

                workers_addrs: una lista de tuplas (ip,port) de los workers que respondieron
                workers_status: un diccionario  de diccionarios que tiene como llave la tupla del addr
                del worker. Las llaves de un diccionario de un worker son:
                    current_task_cap: la capacidad actual de tareas del worker

                    current_job_cap: la capacidad actual de jobs del worker
        '''
        workers_addrs = self.get_all_workers()
        print('JobTracker: ', 'Los workers son: ', workers_addrs)
        # workers_status es un diccionario de worker_addr(ip,port) -> status que es otro diccionario
        workers_status = self._check_workers_status(workers_addrs)
        workers_addrs = list(workers_status.keys())
        print('JobTracker: ', 'Los workers activos son: ', workers_addrs)
        print('JobTracker: ', "Estos son los estados de los workers ", workers_status)
        return workers_addrs, workers_status

    def execute_job(self):
        print('JobTracker: ', "Creamos un proceso para escuchar si me hacen ping")
        self.start_listen_pings()
        print('JobTracker: ', "Verificamos si ese job ya ha sido ejecutado")
        self._check_if_exists_job()

        print('JobTracker: ', "Voy a ejecutar un job ", self.job_phase)
        self.insert_job()

    def insert_job(self):
        print('JobTracker: ', "Insertamos el job en la tabla de status: ",self.status_db_url)
        payload = {
            'job_id': self.job_id,
            'tracker_ip_ping': self.tracker_addr_ping[0],
            'tracker_port_ping': self.tracker_addr_ping[1],
            'answer_ip': self.client_addr[0],
            'answer_port': self.client_addr[1],
            'status_phase': self.status_phase,
            'map_data_url': self.map_data_url,
            'result_url': self.result_data_url,
            'job_state': self.job_phase,
            'data_type': self.data_type
        }
        # salvamos el status del job

        self.status_handler.insert_job_status(self.file_sys_addr,payload)
        self.getting_workers()

    def getting_workers(self):
        # salvamos el status del job
        self.status_phase = mt.task_phases[0]
        to_update = [('status_phase', self.status_phase), ('job_state', self.job_phase)]
        self.status_handler.update_status_row(self.file_sys_addr, 'job', ('job_id', self.job_id), to_update)

        print('JobTracker: ', "Buscamos los workers y sus status")
        workers_addrs, workers_status = self._getting_workers()
        self.setting_slices(workers_addrs, workers_status)

    def setting_slices(self, workers_addrs, workers_status):
        # salvamos el status del job
        print('JobTracker: ',"Salvamos el status del job a slices")
        self.status_phase = mt.task_phases[1]
        to_update = [('status_phase',self.status_phase)]
        self.status_handler.update_status_row(self.file_sys_addr,'job',('job_id',self.job_id),to_update)

        print('JobTracker: ', "Vamos a hacer Getdata y guardar los slices")
        slices_urls,put_byte = self.get_data(self.file_sys_addr,self.job_phase)
        blocks = []
        print('JobTracker: ', "Tenemos los slices urls: ",slices_urls)
        for i,block in enumerate(mt.get_blocks(slices_urls, len(workers_status))):
            if len(block) == 0: continue
            self.status_handler.insert_status_block(self.file_sys_addr,i,block,self.job_phase)
            blocks.append(block)
        print('JobTracker: ', "Picamos en bloques: ",blocks)
        self.send_tasks(blocks,put_byte, workers_addrs,workers_status)

    def send_tasks(self,blocks, put_byte, workers_addrs,workers_status):
        # salvamos el status del job
        print('JobTracker: ', "Salvamos el job en la fase de sendtask")
        self.status_phase = mt.task_phases[2]
        to_update = [('status_phase', self.status_phase)]
        self.status_handler.update_status_row(self.file_sys_addr,'job', ('job_id',self.job_id), to_update)

        print('JobTracker: ', "Voy a mandar los mensajes Task")
        blocks, assign_tasks = self.distribute_slices(blocks,workers_status,
                                                      put_byte=put_byte)
        print('JobTracker: ', "Vamos a esperar por los DONE")
        self.wait_results(assign_tasks,
                          workers_addrs,put_byte)

    def wait_results(self, assign_tasks,workers_addrs,put_byte):
        # salvamos el status del job
        print('JobTracker: ', "Salvamos el status del job a WaitResults")
        self.status_phase = mt.task_phases[3]
        to_update = [('status_phase', self.status_phase)]
        self.status_handler.update_status_row(self.file_sys_addr,'job', ('job_id', self.job_id), to_update)

        print('JobTracker: ', "Voy a Redistribuir los task y esperar por resultados")
        self.wait_for_results(assign_tasks, workers_addrs,
                              put_byte)
        self.getting_results()

    def get_url_results(self):
        # filtramos en los bloques que tengan puesto done y que tengan la misma fase que yo
        block_filters = [('state',mt.slices_states[-1]),('phase',self.job_phase)]
        done_blocks = self.status_handler.get_status_rows(self.file_sys_addr,'block',block_filters)

        results_urls = set()
        for done_block in done_blocks:
            # por cada bloque buscamos sus results
            result_filters = [('block_id',done_block['block_id'])]
            block_result = self.status_handler.get_status_rows(self.file_sys_addr,'block_result',result_filters)
            print("JobTracker: ",'Buscando las url de los resultados tenemos: ')
            print("JobTracker: ", 'block_id: ',done_block['block_id'],' listas_urls: ',block_result)
            for result_url in block_result:
                results_urls.add(result_url['result_url'])
        return list(results_urls)

    def getting_results(self):
        # salvamos el status del job
        print('JobTracker: ', "Salvamos el estado del job a getting results")
        self.status_phase = mt.task_phases[4]
        to_update = [('status_phase', self.status_phase)]
        self.status_handler.update_status_row(self.file_sys_addr,'job', ('job_id', self.job_id), to_update)

        results_urls = self.get_url_results()
        if self.job_phase == self.states[0]:
            print('JobTracker: ', "Nos preparamos para hacer Reduce")
            self.prepare_to_execute_reduce(results_urls)
        else:
            results = self.get_results(results_urls)
            print('JobTracker: ', "Guardo el resultado final")
            self.data_handler.save_split_byte_data(self.file_sys_addr,self.result_data_url, results)

            # salvamos el status del job
            print('JobTracker: ', "Pasamos a estado DONE")
            self.status_phase = mt.task_phases[5]
            to_update = [('status_phase', self.status_phase)]
            self.status_handler.update_status_row(self.file_sys_addr,'job', ('job_id', self.job_id), to_update)

            self.send_result(self.result_data_url)

    def prepare_to_execute_reduce(self, results_urls):
        print("JobTracker: ", '----------------------------------REDUCE PHASE----------------------------------')
        self.job_phase = self.states[1]
        self.get_data = self._get_data_reducer
        self.map_results = results_urls
        self.getting_workers()

    def _get_data_reducer(self,*args):
        put_byte = True
        return self.map_results, put_byte

    def send_result(self, result_url):
        print("JobTracker: ", "Intentemos mandarle el resultado_url al cliente")
        message = mt.Message().set_message("DONE", {'result_url': result_url})
        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, self.client_addr,
                              lambda: print('JobTracker: ',"Can't send results to client", self.client_addr))

        if answer is not None and answer.message_name == 'OK':
            print('JobTracker: ',"EL cliente recibio la url del resultado final")

        self.send_done_to_current_worker(self.current_worker_addr,'job')
        return 0

    def get_results(self, results_urls):
        result = []
        for result_url in results_urls:
            data, _ = self.data_handler.get_pyobj_data(self.file_sys_addr,data_url=result_url)
            # Todo: tengo que verificar el formato de guardar los resultados en el task executor
            result += data
        return result

    def _get_blocks_urls(self, block_filters):
        blocks_status = self.status_handler.get_status_rows(self.file_sys_addr,'block', block_filters)
        if len(blocks_status) == 0:
            return 0,-1
        blocks_urls = self._get_blocks_urls_with_blocks_rows(blocks_status)
        return blocks_urls,blocks_status

    def _get_blocks_urls_with_blocks_rows(self,blocks_status):
        blocks_urls = []
        for block_status in blocks_status:
            print("JobTracker: Busquemos los slices del bloque: ",block_status['block_id'])
            block_slices = self.status_handler.get_status_rows(self.file_sys_addr,'slices_url',
                                                               [('block_id', block_status['block_id'])])
            temp_block = [slice_row['slice_url'] for slice_row in block_slices]
            if len(temp_block) > 0:
                print("JobTracker: Estos son los slices del bloque: ",block_status, '---> ',temp_block)
                blocks_urls.append(temp_block)
            else:
                print("JobTracker: ",'He cargado un bloque vacio: ',block_status['block_id'])
        # print("JobTracker: ","Estos son los bloques: ",blocks_urls)
        return blocks_urls

    def wait_for_results(self,assign_tasks, workers_addrs,
                         put_byte):
        context = zmqa.Context()
        answer_socket = context.socket(zmq.REP)
        try:
            answer_socket.bind('tcp://{}:{}'.format(*self.tracker_addr))
        except zmq.error.ZMQError:
            print("JobTracker: ZMQERROR in wait_for_results")
            answer_socket.bind('tcp://{}:{}'.format(self.tracker_ip,
                                                    mt.get_available_random_port(self.tracker_ip)))
        print('JobTracker: ',"Ahora el tracker espera por respuestas DONE por: ",self.tracker_addr)
        print("JobTracker: ", 'Esperando Done de: ',workers_addrs)
        cnt_answers = 0
        while True:

            ans = mt.loop_tool(self._try_to_recv_done,answer_socket)
            if ans is None:
                submitted_filters = [('state', mt.slices_states[1]), ('phase', self.job_phase)]
                writing_filters = [('state', mt.slices_states[-2]), ('phase', self.job_phase)]

                print("JobTracker: Buscamos los submitted y los writing blocks")
                submitted_blocks_rows = self.status_handler.get_status_rows(self.file_sys_addr,'block',submitted_filters)
                writing_blocks_rows = self.status_handler.get_status_rows(self.file_sys_addr,'block',writing_filters)
                print("JobTracker: ","Ya recibimos los bloques de submitted y writing")

                if len(submitted_blocks_rows) == 0:
                    if len(writing_blocks_rows) == 0:
                        print("JobTracker: ",'Ya todos los bloques estan en DONE')
                        return 0
                    continue

                testing_blocks = self._get_blocks_urls_with_blocks_rows(submitted_blocks_rows)
                if testing_blocks == 0:
                    answer_socket.close()
                    return 0

                # print("JobTracker: ",'ESTOS SON LOS BLOQUES QUE QUEDAN ES ESTADO SUBMITTED: ',testing_blocks)
                indexes_to_remove = []
                for i,block in enumerate(testing_blocks):
                    block_id = submitted_blocks_rows[i]['block_id']

                    worker_assigned_ping = (submitted_blocks_rows[i]['worker_ip'],
                                       submitted_blocks_rows[i]['worker_port'])

                    # Todo: tengo que revisar porqué me dan key error si no se han caido ningun worker
                    print("JobTracker: ","ESTE ES EL WORKER ASSIGN DE BLOCK_ID ",block_id," worker: ",worker_assigned_ping)

                    print("JobTracker: ",'Vamos a hacerle ping a :',worker_assigned_ping,
                          ' con addr ping: ',worker_assigned_ping)

                    answer = mt.loop_tool(mt.try_to_send_message,mt.send_message,mt.Message().set_message("PING"),
                                          worker_assigned_ping,
                                          lambda : print("JobTracker: ",'No me respondio: ',worker_assigned_ping),1)

                    if answer is not None:
                        print("JobTracker: Ya se que este worker sigue pinchando: ",worker_assigned_ping)
                        indexes_to_remove.append(i)
                    else:
                        print("JobTracker: Parece que murio este worker: ", worker_assigned_ping)
                    print("JobTracker: ",'buscamos otro worker')
                print("JobTracker: ",'Ya le hicimo ping a todos, pacemos a reasignar')
                testing_blocks = [testing_blocks[i] for i in range(len(testing_blocks)) if i not in indexes_to_remove]
                print("JobTracker: ", 'ESTOS SON LOS BLOQUES QUE QUEDAN(MOD) En ESTADO SUBMITTED: ', testing_blocks, ' QUE NO ME RESPONDEN')
                if len(testing_blocks) == 0:
                    continue

                workers_addrs, workers_status = self._getting_workers()
                assign_tasks2 = self.send_tasks_messages(testing_blocks,put_byte,workers_status,registered_block=True)
                for x, y in assign_tasks2.items():
                    assign_tasks[x] = y
                continue

            answer = mt.Message().get_message(ans)
            if answer.message_name == 'ERROR':
                # self.send_error_message_to_addr(answer.payload,self.client_addr)
                print("JobTracker: ","Error por parte del worker: ",answer.payload['info'])

            elif answer.message_name == "DONE":

                worker_addr = answer.payload['worker_addr']
                worker_addr = (worker_addr[0], worker_addr[1])
                print('JobTracker: ',"Recibi un Done de: {}:{}".format(*worker_addr))
                message = mt.Message().set_message("OK")
                print("JobTracker: ","Sending OK to: ",worker_addr)
                answer_socket.send_string(str(message))
                cnt_answers += 1

    async def _try_to_recv_done(self,socket,timeout=3.0):
        answer = None
        print("JobTracker: ", ' Waiting for done message')
        try:
            answer = await asyncio.wait_for(self._wait_done(socket),timeout=timeout)
        except asyncio.TimeoutError:
            print("JobTracker: ",' No me respondieron mas done')

        return answer

    async def _wait_done(self,socket):
        print('JobTracker: ', "estamos dentro de la funcion _wait_done")
        answer = await socket.recv_string()
        print('JobTracker: ','Recibimos el answer: ',answer)
        return answer

    def distribute_slices(self, blocks, workers_status,put_byte=False,registered=False):
        '''
        Manda los tasks messages y devuelve la tupla de slices_urls y assign_tasks

        :param blocks:

        :param workers_status:

        :param put_byte:

        :return:
        '''
        r_port = mt.get_available_random_port(self.tracker_ip)

        self.tracker_addr = (self.tracker_ip, r_port)

        assign_tasks = self.send_tasks_messages(blocks, put_byte,workers_status,registered)

        return blocks, assign_tasks

    def _verify_if_errors_in_fs(self, answer, check_for_error=True):
        if answer is None:
            # payload = {
            #     'info': "File System didn't answer",
            #     'type_error': 'FileSysError',
            #     'send_to_client': True
            # }
            # self.send_error_message_to_addr(payload,self.client_addr)
            print("JobTracker: ","El filesystem no me respondio")
            return -1
        elif isinstance(answer,mt.Message) and check_for_error and answer.message_name == 'ERROR':
            error_info = answer.payload['info']
            if 'type_error' in answer.payload.keys():
                type_error = answer.payload['type_error']
                print('FileSystemError: ',error_info)
                print('TypeError: ',type_error)
                if type_error == 'LockFileError':
                    return 1
            self.send_error_message_to_addr(answer.payload,self.client_addr)

    def _reset_method_if_no_answer_from_fs(self,f,*args):
        print("JobTracker: ",'Vamos a resetear la tarea: ',f)
        if len(self.file_sys_addrs) == 0:
            self._update_filesystem_nodes()

        self.file_sys_addr = self._get_new_filesystem_node()
        print("JobTracker: ", 'Reseteamos la tarea: ',f)
        return f(self.file_sys_addr,*args[1:])

    def _get_new_filesystem_node(self):
        import random
        assert len(self.file_sys_addrs) != 0, "No existen nodos del filesystem"
        r = random.randrange(0,len(self.file_sys_addrs))
        return self.file_sys_addrs.pop(r)

    # Todo: areglar esto pues no existe el register
    def _update_filesystem_nodes(self):
        print("Worker: ", 'Mandamos a buscar los nodos del filesystem')
        filesystem_nodes = mt.send_broadcastmessage(self.filesystem_broadcast_addr)
        print("Worker: ", "Estos son los nodos del filesystem: ", filesystem_nodes)
        self.file_sys_addrs = filesystem_nodes
        if len(filesystem_nodes) == 0:
            print("Worker: ", 'no me respondieron los nodos del filesystem')
            exit()
        return 0

    def send_tasks_messages(self, blocks, put_byte,workers_status,registered_block=False):
        print("JobTracker: ",'Vamos a repartir los bloques: ',blocks)
        import heapq
        # este es el heap donde cada elemento es una tupla (current_task_cap,current_job_cap,worker_addr)
        # el heap es de minimo luego tuve que guardar los valores en negativo para hacerlo de maximo.
        # esto es una estimacion de trabajo, todos sabemos que no es real en la práctica
        workers = [(-workers_status[addr]['current_task_cap'],
                    -workers_status[addr]['current_job_cap'], addr) for addr in workers_status]
        heapq.heapify(workers)
        # blocks = list(mt.get_blocks(slices_urls,len(workers_status)))
        # aqui guardamos key:slice_url,value:worker_addr
        assign_tasks = {}
        current_block_index = 0
        while current_block_index < len(blocks):

            try:
                worker = workers[0][2] #self._get_active_worker(veto_workers, workers_addrs, workers_status)
            except IndexError:
                payload = {
                    'info':"No workers availables"
                }
                _,workers_status = self._getting_workers()
                if len(workers_status) == 0:
                    self.send_error_message_to_addr(payload,self.client_addr)
                    return -1
                workers = [(-workers_status[addr]['current_task_cap'],
                            -workers_status[addr]['current_job_cap'], addr) for addr in workers_status]
                heapq.heapify(workers)
                continue

            list_urls = blocks[current_block_index]

            if registered_block:
                block_id = self.status_handler.get_block_id_of_slice(self.file_sys_addr, list_urls[0])
            else:
                block_id = '{}_{}'.format(self.job_phase, current_block_index)

            answer, _ = self._send_single_task_message(list_urls, put_byte, worker,block_id)
            print("JobTracker :","Me respondieron al task: ",answer)
            if answer is None or \
                    (isinstance(answer,mt.Message)
                     and answer.message_name == 'ERROR'):
                print("JobTracker: ","No me respondio el worker: ",worker)
                heapq.heappop(workers)
                continue
            # print("JobTracker: ",'Vamos a asignar a cada slice el worker: ',worker)

            worker_ping_addr = (answer.payload['task_ping_ip'], answer.payload['task_ping_port'])

            to_update = [('state', mt.slices_states[1]), ('worker_ip', worker_ping_addr[0]),
                         ('worker_port', worker_ping_addr[1])]

            print("JobTracker: ", "Salvamos el estado del bloque {} a SUBMITTED en el filesys: {}"
                  .format(block_id, self.file_sys_addr))

            self.status_handler.update_status_row(self.file_sys_addr, 'block', ('block_id', block_id), to_update)
            print("JobTracker: ", '???? Al worker ', worker, ' le asociamos el bloque: ', block_id)

            assign_tasks[block_id] = worker_ping_addr
            current_block_index += 1
            worker_info = heapq.heappop(workers)
            worker_info_data = (worker_info[0]+1,worker_info[1])
            heapq.heappush(workers,(*worker_info_data,worker))

        return assign_tasks

    def _send_single_task_message(self, block_urls, put_byte, worker,block_id):
        message = mt.Message()
        payload = {
            'job_url': self.job_url,
            'job_id': self.job_id,
            'function_url': self.functions_url,
            'block_urls': block_urls,
            'task': self.job_phase,
            'answer_addr': self.tracker_addr,
            'load_byte': put_byte,
            'status_db_url':self.status_db_url,
            'block_id':block_id
        }
        message.set_message("TASK", payload)
        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message, worker,
                              lambda: print('JobTracker: ', "No me respondio el worker ", worker))
        return answer,block_urls

    def _get_active_worker(self, veto_workers, workers_addrs,workers_status):
        for worker in workers_addrs:
            if worker not in veto_workers:
                return worker
        return None

    def get_all_workers(self):
        print("JobTracker: ",'Preguntamos por los workers')
        workers = mt.send_broadcastmessage(self.worker_broadcast_addr)
        print("JobTracker: ","Obtuvimos, workers: ",workers)
        return workers

    def send_error_message_to_addr(self, payload, addr):
        error_message = mt.Message().set_message('ERROR', payload)
        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, error_message, addr,
                              lambda: print('JobTracker: ', 'No me respondio: ',addr))
        if answer is not None:
            print('JobTracker: ', 'Me respondio: ',addr)

        self.send_done_to_current_worker(self.current_worker_addr, 'job')

    def send_done_to_current_worker(self,current_worker_addr,role):
        # Le mandamos a mi propio worker que terminamos la tarea
        print('JobTracker: ', 'Mandamos DONE al propio worker')
        message = mt.Message().set_message('DONE', {'role': role})
        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message,
                              current_worker_addr,
                              lambda: print('JobTracker: ', 'No me respondio mi propio worker'))

        if answer is not None:
            print('JobTracker: ','Termine')
        print("JobTracker: ", "Le hice terminate al pinging process")
        self.pinging_process.join()
        exit()

    def _check_workers_status(self,workers):
        workers_status = {}

        for worker in workers:
            print('JobTracker: ', 'Tratemos de contactar al worker: ',worker)

            answer = mt.loop_tool(mt.try_to_send_message,mt.send_message,mt.Message().set_message("STATUS"),worker,
                                  lambda :print('JobTracker: ','No me respondio el worker {}:{}'.format(*worker)))

            if answer is not None and answer.message_name == "OK":
                workers_status[(worker[0],worker[1])] = answer.payload
                print('JobTracker: ', 'Me respondio el worker {}:{}'.format(*worker))

        if len(workers_status) == 0:

            messsage_error = mt.Message().set_message('ERROR',{
                'info': "Can execute job, there is not workers"
            })
            answer = mt.loop_tool(mt.try_to_send_message,mt.send_message,messsage_error,self.client_addr,
                                  lambda :print('JobTracker: ','No me respondio el cliente al '
                                                            'mensaje de error'))

            print('JobTracker: ', 'No me respondio nadie, cerrando JobTracker')
            self.send_done_to_current_worker(self.current_worker_addr,'job')

        print('JobTracker: ', 'este es el workerstatus: ',workers_status)
        return workers_status
