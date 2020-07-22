import message_tools as mt
import pickle
import time


class DataHandler:
    def __init__(self,job_url,job_id,verify_if_errors_in_fs_func,_reset_method_if_no_answer_from_fs):
        self._verify_if_errors_in_fs = verify_if_errors_in_fs_func
        self._reset_method_if_no_answer_from_fs = _reset_method_if_no_answer_from_fs
        self.job_url = job_url
        self.job_id = job_id

    def get_line_by_line_str(self, file_sys_addr, job_phase, data_file_name='map_data', data_url=None,len_limit=500):
        if data_url is None:
            file_url = "{}/{}".format(self.job_url, data_file_name)
        else:
            file_url = data_url

        # print("Mandamos a hacer getdata al file: ",file_url)
        message = mt.Message()
        message.set_message("GETDATA", {'file_url': file_url,
                                        'byte': False})

        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, file_sys_addr,
                                        lambda: print("File system didn't respond"))

        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.get_line_by_line_str,file_sys_addr,job_phase,
                                                           data_file_name,data_url,len_limit)
        if answer.message_name == 'OK':
            new_addr = (answer.payload['ip'], answer.payload['port'])

            message = mt.Message()
            message.set_message("OK")
            slices_url = []
            i = 0
            # print('Intentamos obtener las lineas')
            slice_,eof = self._get_lines(message, new_addr,len_limit)
            # print("Getlines devolvio: {},{}".format(slice_,eof))
            if slice_ == -1:
                return self._reset_method_if_no_answer_from_fs(self.get_line_by_line_str,file_sys_addr,job_phase,
                                                               data_file_name,data_url,len_limit)
            slices_url.append(self._save_split_get_url(file_sys_addr, i, job_phase, slice_))
            # print("Intentamos obtener las lineas en el while del get_line_by_line")
            while not eof:
                slice_,eof = self._get_lines(message,new_addr,len_limit)
                # print("Getlines devolvio: {},{}".format(slice_, eof))
                if slice_ == -1:
                    return self._reset_method_if_no_answer_from_fs(self.get_line_by_line_str, file_sys_addr, job_phase,
                                                                   data_file_name, data_url, len_limit)
                i += 1
                slices_url.append(self._save_split_get_url(file_sys_addr, i, job_phase, slice_))
            is_byte = False
            return slices_url, is_byte

    def _save_split_get_url(self, file_sys_addr, i, job_phase, slice_):
        slice_url = '{}/{}_{}_slice'.format(self.job_url, i, job_phase)
        # print('Intentamos salvar el split: ', slice_url)
        self.save_split_str_data(file_sys_addr, slice_url, slice_)
        # print('Salvamos el split: ', slice_url)
        return slice_url

    def get_str_data(self, file_sys_addr,data_file_name='map_data', data_url=None):

        # print( 'Voy a buscar un str_data')

        if data_url is None:
            file_url = "{}/{}".format(self.job_url, data_file_name)
        else:
            file_url = data_url

        message = mt.Message()
        message.set_message("GETDATA", {'file_url': file_url,
                                        'byte': False})
        # print('Mandamos el mensaje de GETDATA')
        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, file_sys_addr,
                                        lambda: print("File system didn't respond"))
        # print( 'Me respondieron: ',answer)
        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.get_str_data,file_sys_addr,data_file_name,data_url)
        elif if_none == 1:
            time.sleep(0.5)
            print("Intentamos de nuevo get_str_data")
            return self.get_str_data(file_sys_addr,data_file_name,data_url)

        if answer.message_name == 'OK':
            data_and_is_byte = self._get_complete_object(answer,'',mt.send_message_recv_str)
            if data_and_is_byte == -1:
                return self._reset_method_if_no_answer_from_fs(self.get_str_data, file_sys_addr, data_file_name,
                                                               data_url)
            data,is_byte = data_and_is_byte
            # print( "Este es el data que me mandaron: ",data)
            return data,is_byte
        return -1

    def save_splits_byte_data(self, file_sys_addr,slices, slices_urls,job_phase):

        for i, sc in enumerate(slices):
            slice_url = "{}/{}_slice{}".format(self.job_url, job_phase, i)
            slices_urls.append(slice_url)
            self.save_split_byte_data(file_sys_addr,slice_url, sc)
        return 0

    def save_split_byte_data(self, file_sys_addr,data_url, data):
        # print( "Esto es lo que voy a mandar a buscar: ",data_url)
        message = mt.Message()
        message.set_message("PUTDATA", {'file_url': data_url,
                                        'byte': True,
                                        })
        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, file_sys_addr,
                                        lambda: print("No me contesta el File Sys"))
        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.save_split_byte_data,file_sys_addr,data_url,data)
        if if_none == 1:
            time.sleep(0.5)
            print("Intentamos de nuevo save_split_byte_data")
            return self.save_split_byte_data(file_sys_addr,data_url,data)

        if answer.message_name == 'OK':
            new_addr = (answer.payload['ip'],answer.payload['port'])

            answer = mt.loop_tool(mt.try_send_byte_data,mt.send_byte_data, data, new_addr,
                                           lambda: print("file sys did't respond"))

            if_none = self._verify_if_errors_in_fs(answer)
            if if_none == -1:
                return self._reset_method_if_no_answer_from_fs(self.save_split_byte_data, file_sys_addr, data_url, data)

            answer = mt.loop_tool(mt.try_to_send_message,mt.send_byte_data, b'', new_addr,
                                           lambda: print("file sys did't respond"))
            #
            # if_none = self._verify_if_errors_in_fs(answer)
            # if if_none == -1:
            #     return self._reset_method_if_no_answer_from_fs(self.save_split_byte_data, file_sys_addr, data_url, data)

            # print( "Guardamos el archivo: ",data_url)
            return 0

    def save_splits_str_data(self, file_sys_addr,slices, slices_urls,job_phase):

        for i, sc in enumerate(slices):
            slice_url = "{}/{}_slice{}".format(self.job_url, job_phase, i)
            slices_urls.append(slice_url)
            self.save_split_str_data(file_sys_addr,slice_url, sc)
        return 0

    def save_split_str_data(self, file_sys_addr,slice_url, data):
        message = mt.Message()
        message.set_message("PUTDATA", {'file_url': slice_url,
                                        'byte': False
                                        })

        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, file_sys_addr,
                                        lambda: print("No me contesta el File Sys"))

        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.save_split_str_data, file_sys_addr, slice_url, data)
        if if_none == 1:
            time.sleep(0.5)
            print("Intentamos de nuevo save_split_str_data")
            return self.save_split_str_data(file_sys_addr,slice_url,data)

        if answer.message_name == 'OK':
            new_addr = (answer.payload['ip'], answer.payload['port'])
            message = data
            answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, new_addr,
                                            lambda: print("file sys did't respond"))
            if_none = self._verify_if_errors_in_fs(answer)
            if if_none == -1:
                return self._reset_method_if_no_answer_from_fs(self.save_split_str_data, file_sys_addr, slice_url, data)

            answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, '', new_addr,
                                  lambda: print( "file sys did't respond"))
            # if_none = self._verify_if_errors_in_fs(answer)
            # if if_none == -1:
            #     return self._reset_method_if_no_answer_from_fs(self.save_split_str_data, file_sys_addr, slice_url, data)

        else:
            # print("Error when saving slice: ", data)
            return -1

    def save_block(self,file_sys_addr,data_urls,pairs,status_db_url,block_id):
        block_size = len(data_urls)
        message = mt.Message()
        message.set_message("APPENDBLOCK", {'block_size': block_size,
                                            'status_db_url': status_db_url,
                                            'block_id': block_id})

        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message, file_sys_addr,
                              lambda: print("No me contesta el File Sys"))

        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.save_block, file_sys_addr, data_urls,
                                                           pairs,status_db_url,block_id)

        new_addr = (answer.payload['ip'], answer.payload['port'])
        message.set_message("APPENDBLOCK", {'data_urls': data_urls,
                                            'pairs': pairs
                                            })

        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message, new_addr,
                              lambda: print("No me contesta el File Sys"))
        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.save_block, file_sys_addr, data_urls,
                                                           pairs,status_db_url,block_id)

        if isinstance(answer,mt.Message) and answer.message_name == 'OK':
            return 0
        return self._reset_method_if_no_answer_from_fs(self.save_block, file_sys_addr, data_urls,
                                                       pairs,status_db_url,block_id)

    def save_key_file(self, file_sys_addr,data_url, key_value_pair):
        '''
        Manda a hacer append al filesystem el objeto key_value_pair en el archivo data_url
        :param data_url: la url del archivo que se le va a hacer append
        :param key_value_pair: par (key,[value]). Se le pone el value en una lista para que al
                            filesystem cuando le manden append haga += para concatenar las listas
        :return: 0
        '''
        # supongamos que podamos hacer APPENDDATA payload {file_url: blabla, byte: true}
        # el file sys tiene que hacerle pickle loads a lo que le mande (data0 = pickle.loads(data))
        # el file sys tiene que hacer lock, luego  hacer data1 = pickle.load(file)
        # con data en la mano hacer data1[1]+= data0
        # y luego pickle.dump(data1,file)
        message = mt.Message()
        message.set_message("APPENDDATA", {'file_url': data_url,
                                           'byte': True})

        answer = mt.loop_tool(mt.try_to_send_message, mt.send_message, message, file_sys_addr,
                              lambda: print("No me contesta el File Sys"))

        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return -1
        elif if_none == 1:
            print('Me dijeron que esta puesto un lock en :',data_url)
            print("Intentamos de nuevo hacer appenddata a: ",data_url)
            time.sleep(0.5)
            return self.save_key_file(file_sys_addr, data_url, key_value_pair)

        # print('Ahora mandare a escribir en este archivo: ', data_url)
        if answer.message_name == 'OK':
            new_addr = (answer.payload['ip'], answer.payload['port'])
            # print("DataHandler->>>: este es el key value pair que intentamos mandar: ",key_value_pair)
            answer = mt.loop_tool(mt.try_send_byte_data, mt.send_byte_data, key_value_pair, new_addr,
                                  lambda: print(" file sys did't respond"))

            if_none = self._verify_if_errors_in_fs(answer)
            if if_none == -1:
                return self._reset_method_if_no_answer_from_fs(self.save_key_file, file_sys_addr, data_url,
                                                               key_value_pair)

            # answer = mt.loop_tool(mt.try_to_send_message, mt.send_byte_data, b'', new_addr,
            #                       lambda: print('Task_Exec: ', "file sys did't respond"))
            #
            # if_none = self._verify_if_errors_in_fs()
            # if if_none == -1:
            #     return self._reset_method_if_no_answer_from_fs(self.save_key_file, file_sys_addr, data_url,
            #                                                    key_value_pair)

            # print("Guardamos el archivo: ", data_url)
        return 0

    # Todo: tengo que poner al task executor que si se cae el nodo del filesystem, se recupere y busque otro nodo
    def get_pyobj_data(self, file_sys_addr,data_file_name='map_data', data_url=None):
        if data_url is None:
            file_url = "{}/{}".format(self.job_url, data_file_name)
        else:
            file_url = data_url

        message = mt.Message()
        message.set_message("GETDATA", {'file_url': file_url,
                                        'byte': True})

        answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, file_sys_addr,
                                        lambda: print("File system didn't respond"))

        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return self._reset_method_if_no_answer_from_fs(self.get_pyobj_data,file_sys_addr,data_file_name,data_url)
        elif if_none == 1:
            time.sleep(0.5)
            print("Intentamos de nuevo get_pyobj_data")
            return self.get_pyobj_data(file_sys_addr,data_file_name,data_url)

        if answer.message_name == 'OK':
            data_and_is_byte = self._get_complete_object(answer, b'',mt.send_message_recv_byte)
            if data_and_is_byte == -1:
                return self._reset_method_if_no_answer_from_fs(self.get_pyobj_data,file_sys_addr,data_file_name,data_url)
            data, is_byte = data_and_is_byte
            # print( 'Este es el objeto que me mandaron a buscar: ',data)
            return data, is_byte

    def _get_complete_object(self, answer,end_character,send_message_function):
        new_addr = (answer.payload['ip'], answer.payload['port'])
        message = mt.Message()
        message.set_message("OK")

        slice_ = end_character
        tem_slice = end_character
        # print( 'Tratamos de traer la mayor cantidad de lineas')
        _result,eof = self._get_lines(message, new_addr, empty_chac=end_character, send_message_func=send_message_function)
        if _result == -1:
            return _result
        tem_slice += _result
        slice_ += tem_slice
        # print( 'Trajimos el primer trozo: ',slice_)

        while not eof:
            tem_slice = end_character
            _result,eof = self._get_lines(message, new_addr, empty_chac=end_character,
                                      send_message_func=send_message_function)
            if _result == -1:
                return _result
            tem_slice += _result
            slice_ += tem_slice

        if end_character == b'':
            is_byte = True
            data = pickle.loads(slice_)
            return data, is_byte
        return slice_,False

    def _get_lines(self,message,new_addr,len_limit=500,empty_chac='',send_message_func=mt.send_message_recv_str):
        slice_ = empty_chac

        answer = mt.loop_tool(mt.try_to_send_message, send_message_func, message, new_addr,
                              lambda: print("File SYS didn't respond"))

        if_none = self._verify_if_errors_in_fs(answer)
        if if_none == -1:
            return if_none,False

        while len(slice_) < len_limit and answer != empty_chac:
            slice_ += answer
            answer = mt.loop_tool(mt.try_to_send_message, send_message_func, message, new_addr,
                                  lambda: print( "File SYS didn't respond"))
            if_none = self._verify_if_errors_in_fs(answer)
            if if_none == -1:
                return if_none,False

        if len(slice_) >= len_limit:
            slice_ += answer

        # print("--->Data Handler: ",'En el get lines me llego este slice: ',slice_)
        eof = answer == empty_chac
        return slice_, eof


