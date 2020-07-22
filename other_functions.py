# def _get_str_data(self, data_file_name='map_data', data_url=None):
#     if data_url is None:
#         file_url = "{}/{}".format(self.job_url, data_file_name)
#     else:
#         file_url = data_url
#
#     message = mt.Message()
#     message.set_message("GETDATA", {'file_url': file_url,
#                                     'byte': False})
#     data = None
#
#     answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, self.file_sys_addr,
#                                     lambda: print('JobTracker: ',"File system didn't respond"))
#
#     self._verify_if_errors_in_fs(answer)
#
#     new_addr = (answer.payload['ip'], answer.payload['port'])
#
#     message = mt.Message()
#     message.set_message("OK", {})
#
#     answer = mt.loop_tool(mt.try_to_send_message,mt.send_message, message, new_addr,
#                                     lambda: print('JobTracker: ',"File SYS didn't respond"))
#
#     self._verify_if_errors_in_fs(answer)
#
#     if answer.message_name == 'DATA':
#         data = answer.payload['data']
#
#     is_byte_data = False
#     print('JobTracker: ',"this is the data: ", data)
#     return data, is_byte_data
#
# def _distribute_text(self, data, workers_status):
#     '''
#     este metodo por ahora el criterio de distribucion es: coge un texto y pícalo en slices de igual tamaño
#     :param data: el dato en si
#     :param workers_status: ahora mismo es un diccionario que tiene nada mas que ip y puerto como llaves
#     :return: cada uno de los slices
#     '''
#
#     max_cnt_slices = len(workers_status)
#     max_data_slice_len = int(len(data) / max_cnt_slices)
#     print('JobTracker: ',max_data_slice_len)
#     start_i = 0
#     slices = []
#     continue_reading = True
#     delimiters = self.delimiters
#     while continue_reading:
#
#         j = start_i + max_data_slice_len
#         print('JobTracker: ','tomamos a j:{}'.format(j))
#
#         while True:
#
#             if j < len(data):
#                 if data[j] not in delimiters:
#                     j += 1
#                     print('JobTracker: ','incremento j ', j)
#                 else:
#                     slices.append((start_i, j))
#                     start_i = j + 1
#                     print('JobTracker: ','encontre un slice ', slices[-1], data[slices[-1][0]:slices[-1][1]])
#                     break
#             else:
#                 continue_reading = False
#                 slices.append((start_i, j))
#                 print('JobTracker: ','encontre un slice ', slices[-1], data[slices[-1][0]:slices[-1][1]])
#                 break
#
#     return [data[i:j] for i, j in slices]