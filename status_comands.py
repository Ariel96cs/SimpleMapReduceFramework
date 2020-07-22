import message_tools as mt


class StatusHandler:
    def __init__(self,status_db_url,verify_error_func,_reset_method_if_no_answer_from_fs):
        self.status_db_url = status_db_url
        self._verify_if_errors_in_fs = verify_error_func
        self._reset_method_if_no_answer_from_fs = _reset_method_if_no_answer_from_fs

    def get_status_rows(self,file_sys_addr, table_name, filters=[], catch_error=True):
        '''
        Manda a buscar en el self.status_db_url en el filesystem, en la tabla table_name, las filas
         que cumplan con los filtros filters

        :param file_sys_addr:

        :param catch_error: boobleano para decidir si se captura el error en la funcion o no

        :param table_name: el nombre de la tabla en la cual buscamos las filas

        :param filters: lista de tuplas(field_name,value),osea, where field_name == value

        :return: lista de los resultados, la cual es una lista de diccionarios con los campos
        '''
        # print("Request for {}'s rows".format(table_name))
        first_message = mt.Message()
        first_message.set_message('GETROWS', {'status_db_url': self.status_db_url})
        second_message = mt.Message().set_message('GETROWS', {'table': table_name, 'filters': filters})
        answer = self.send_status_command(file_sys_addr,first_message, second_message, True)
        if catch_error and answer != -1:
            result_ = self._verify_if_errors_in_fs(answer,catch_error)
            if result_ == -1:
                return self._reset_method_if_no_answer_from_fs(self.get_status_rows,
                                                               file_sys_addr,
                                                               table_name,
                                                               filters,
                                                               catch_error)
        elif answer == -1:
            return self._reset_method_if_no_answer_from_fs(self.get_status_rows,
                                                           file_sys_addr,
                                                           table_name,
                                                           filters,
                                                           catch_error)

        elif isinstance(answer, mt.Message) and answer.message_name == 'ERROR':
            return -1

        lines = answer.payload['rows']
        # print("Estas son las filas que me mando el filesys de: {} :{}"
        #       .format(self.status_db_url, lines))
        # print("Got lines: ",lines)
        return lines

    def put_done_block_in_db(self,file_sys_addr,slices_result_urls,block_urls):
        a_slice_url = block_urls[0]
        # print("Vamos actualizar el state de un bloque a DONE")
        # buscamos el id del bloque correnspondiente
        slice_row = self.get_status_rows(file_sys_addr,'slices_url',[('slice_url',a_slice_url)])[0]
        block_id = slice_row['block_id']
        block_row = self.get_status_rows(file_sys_addr,'block',[('block_id',block_id)])[0]
        print("put_done!!!!!!!!!!!!",block_row)
        if block_row['state'] == mt.slices_states[-1]:
            print('Ya estaba listo este bloque: ',block_id)
            return -1, True
        print("Pongamos en DONE al bloque: ",block_id)
        self.insert_block_result(file_sys_addr, block_id, slices_result_urls), False
        self.update_status_row(file_sys_addr, 'block', ('block_id', block_id), [('state', mt.slices_states[-1])])
        print("El estado de {} ahora es DONE".format(block_id))
        print("!!!!!!!!!Seguro al Done: ", self.get_status_rows(file_sys_addr, 'block', [('block_id', block_id)])[0])
        return

    def get_block_id_of_slice(self,file_sys_addr,a_slice_url):
        slice_row = self.get_status_rows(file_sys_addr, 'slices_url',
                                         [('slice_url', a_slice_url)], catch_error=False)[0]
        if slice_row == -1:
            return -1
        block_id = slice_row['block_id']
        return block_id

    def check_if_block_is_done(self,file_sys_addr,a_slice_url):
        # buscamos el id del bloque correnspondiente
        block_id = self.get_block_id_of_slice(file_sys_addr,a_slice_url)
        if block_id == -1:
            return -1
        print("DataHandler->>>> Verificar el estado del bloque: ",block_id)
        block_row = self.get_status_rows(file_sys_addr, 'block', [('block_id', block_id)],catch_error=False)[0]
        if block_row == -1:
            return -1
        print("check_done!!!!!!!!!!!!", block_row)
        block_state = block_row['state']
        if block_state == mt.slices_states[-1] or block_state == mt.slices_states[2]:
            print('Ya estaba listo este bloque: ', block_row)
            return True,block_id
        print("No estaba listo el bloque: ",block_row)
        return False,block_id

    def insert_block_result(self,file_sys_addr, block_id, result_urls):
        '''
        Le asigna al block_id de esta fase, cada uno de los results url
        :param file_sys_addr: el address del nodo del filesystem

        :param block_id: el id del bloque en la tabla block
        :param result_urls: la lista de las url de los resultados del bloque
        :return:
        '''
        first_message = mt.Message()
        first_message.set_message('BLOCKRESULT', {'status_db_url': self.status_db_url})
        second_message = mt.Message().set_message('BLOCKRESULT', {'block_id': block_id,
                                                         'result_url': result_urls})
        # print("Insertemos el resultado del bloque: ",block_id)
        result_ = self.send_status_command(file_sys_addr,first_message,second_message)
        if result_ == -1:
            return self._reset_method_if_no_answer_from_fs(self.insert_block_result,file_sys_addr,block_id,result_urls)
        print("Insertado el resultado del bloque: ",block_id)

        return 0

    def insert_status_block(self, file_sys_addr,block_id, slices_urls, job_phase):
        '''
        Inserta en la tabla de Block un nuevo bloque con state=self.state y le asigna
        los slices_urls

        :param file_sys_addr:

        :param job_phase: 'map' o 'reduce'

        :param block_id: el numero del bloque respecto al piquete en bloques de la entrada

        :param slices_urls: la lista de las urls de los slices

        :return: 0 si todo fue ok
        '''

        first_message = mt.Message()
        first_message.set_message('INSERTBLOCK', {'status_db_url': self.status_db_url})
        block_id = '{}_{}'.format(job_phase, block_id)
        print("Vamos a insertar el bloque: ",block_id)
        second_message = mt.Message().set_message('INSERTBLOCK', {'block_id': block_id,
                                                                  'state': mt.slices_states[0],
                                                                  'slices_id': slices_urls,
                                                                  'phase': job_phase})
        result_ = self.send_status_command(file_sys_addr,first_message, second_message)
        if result_ == -1:
            return self._reset_method_if_no_answer_from_fs(self.insert_status_block,
                                                           file_sys_addr,
                                                           block_id,
                                                           slices_urls,
                                                           job_phase)
        return 0

    def update_status_row(self, file_sys_addr,table_name, key_row, row_updates):
        '''
        Actualiza de la table_name la fila con llave key los campos row_updates[0] por row_updates[1]

        :param file_sys_addr:

        :param table_name: el nombre de la tabla qu queremos actulizar

        :param key_row: (key_name,value) la llave de la fila que queremos actualizar

        :param row_updates: lista de tuplas (field_name,new_value)

        :return: 0 si termina en en talla
        '''
        # print("Vamos a hacerle update a la tabla: {}, el key:{} las modificaciones: {}"
        #       .format(table_name,key_row,row_updates))

        first_message = mt.Message()
        first_message.set_message('UPDATEROW', {'status_db_url': self.status_db_url})
        second_message = mt.Message().set_message('UPDATEROW', {'table': table_name,
                                                                'key': key_row,
                                                                'fields_values': row_updates})
        result_ = self.send_status_command(file_sys_addr,first_message, second_message)
        if result_ == -1:
            # print("Parece que no me respondio el filesystem: ",file_sys_addr)
            return self._reset_method_if_no_answer_from_fs(self.update_status_row,
                                                           file_sys_addr,
                                                           table_name,
                                                           key_row,
                                                           row_updates)
        # print("Updated status: table:{}, key: {}, updates:{}".format(table_name,key_row,row_updates))
        return 0

    def insert_job_status(self, file_sys_addr,payload):
        '''
        Inserta an la tabla job los datos del job que estan en el payload

        :param file_sys_addr:

        :param payload:  es un diccionario que tiene como campos:

        payload{
            'status_db_url':la url del db,
            'job_id' que es la llave:value,
            'tracker_ip_ping':value,
            'tracker_port_ping':value,
            'answer_ip':value,
            'answer_port':value,
            'status_phase':value(getworkers,slice,waitforresults,...),
            'map_data_url':value,
            'result_url':value,
            'job_state':value(map o reduce),
            'data_type':value
        }

        :return: 0 si todo en talla
        '''
        first_message = mt.Message()
        first_message.set_message('INSERTJOB', {'status_db_url': self.status_db_url})
        second_message = mt.Message().set_message('INSERTJOB', payload)

        result_ = self.send_status_command(file_sys_addr,first_message, second_message)
        if result_ == -1:
            return self._reset_method_if_no_answer_from_fs(self.insert_job_status,file_sys_addr,payload)
        print("Inserted Job")
        return 0

    def send_status_command(self,file_sys_addr,first_message,second_message,get_result=False):
        # print("Trying to send status command")

        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, first_message, file_sys_addr,
                              lambda: print("File sys didn't answer"))
        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return -1
        # print("Sended first message")

        new_addr = (answer.payload['ip'], answer.payload['port'])

        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, second_message, new_addr,
                              lambda: print("File sys didn't answered"))

        if get_result:
            return answer
        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return -1
        # print("Sended second message")
        return 0
