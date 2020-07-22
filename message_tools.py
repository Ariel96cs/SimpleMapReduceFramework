import zmq
import json
import pickle
import random
import asyncio
import zmq.asyncio as zmqa
import timeout_decorator

status_csv_field_names = [
    'job_id', 'tracker_ip_ping','tracker_port_ping',
    'answer_ip','answer_port','status_phase',
    'map_csv_url','reduce_csv_url','map_data_url',
    'result_url','job_state','data_type'
]
slices_csv_field_names = [
    'slice_url','state','result_url'
]
slices_states = [
    'ADDED',
    'SUBMITTED',
    'WRITING',
    'DONE'
]
task_phases = ['GETWORKERS',
                'SLICES',
                'SENDTASK',
                'WAITANSWERS',
                'GETRESULTS',
                'DONE']

class Message:
    def __init__(self):
        self.message_name = ""
        self.payload_len = ""
        self.payload = ""

    def get_message(self, message):
        self.message_name, self.payload_len, self.payload = self._get_prop(message)
        return self

    @staticmethod
    def _get_prop(message):
        # print('Tratamos de cargar este mensaje: ',message)
        message_split = message.split(" ", 2)
        name = message_split[0]
        payload_len = int(message_split[1])
        payload = {}
        if payload_len > 0 and len(message_split) == 3:
            # payload = self._group_list(message[2:])
            payload = json.loads(message_split[2])
        # else:
        #     payload = "Error\n Nothing sent"

        return name, payload_len, payload

    def set_message(self, name, payload_dict={}):
        self.message_name = name
        self.payload = payload_dict
        self.payload_len = len(self.payload)
        # print("seteamos este mensaje: ",self)
        return self

    @staticmethod
    def _group_list(message):
        b = ""
        for x in message:
            b += x
        return b

    def __str__(self):
        return "{} {} {}".format(self.message_name, self.payload_len, json.dumps(self.payload))

async def try_to_send_message(send_message_func, message, message_recv, except_action, timeout=8.0):
    answer = None
    try:
        answer = await asyncio.wait_for(send_message_func(message,message_recv),timeout=timeout)
        # print('en el try_send_message me respondieron esto :', answer)
    except asyncio.TimeoutError:
        except_action()
        # print('timeout! en el try_to_send_message')
    return answer

async def try_to_recv(recv_function,timeout=5):
    try:
        answer = await asyncio.wait_for(recv_function(), timeout=timeout)
    except asyncio.TimeoutError:
        return -1
    return Message().get_message(answer)

async def raw_try_to_recv(recv_function,timeout=5):
    try:
        answer = await asyncio.wait_for(recv_function(), timeout=timeout)
    except asyncio.TimeoutError:
        return -1
    return answer


def recv_ping_broadcast(ping_addr,current_addr):
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', int(ping_addr[1])))
    print('Listening on :',ping_addr)
    print('CurrentAddr: ',current_addr)
    current_addr_byte = pickle.dumps(current_addr)
    while True:
        data, addr = sock.recvfrom(1024)
        print("Worker: Recibí ping de: ", addr)
        # todo: del otro lado vamos a hacerle pickle load
        sock.sendto(current_addr_byte, 0, addr)


def send_broadcastmessage(broadcast_addr):
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    print("Sending broadcast on: ",broadcast_addr)
    sock.sendto(b'Any worker there?', 0, (broadcast_addr[0],int(broadcast_addr[1])))
    workers = []

    while True:
        try:
            data, addr = raw_recv_sock(sock.recvfrom)
            new_addr = pickle.loads(data)
            workers.append((str(new_addr[0]), str(new_addr[1])))
            print("Me respondio el broadcast el nodo:", addr)
        except TimeoutError:
            print("Estos me respondieron: ", workers)
            break
    return workers


def raw_recv(recv_function,timeout=2):
    @timeout_decorator.timeout(timeout, timeout_exception=TimeoutError)
    def executor_timeout(rcv_f):
        return rcv_f()
    return executor_timeout(recv_function)


def raw_recv_sock(recv_function,buffsize=1024,timeout=2):
    @timeout_decorator.timeout(timeout, timeout_exception=TimeoutError)
    def executor_timeout(rcv_f,bfsize):
        return rcv_f(bfsize)
    return executor_timeout(recv_function,buffsize)

def recv_message(recv_function,timeout=10):
    @timeout_decorator.timeout(timeout, timeout_exception=TimeoutError)
    def executor_timeout(recv_f):
        answer = recv_f()
        answer_message = Message().get_message(answer)
        return answer_message
    return executor_timeout(recv_function)


async def send_message_recv_pyobj(message,addr):
    '''
    Este metodo manda el mensaje y espera que le manden un objeto byte por la red

    :param message:
    :param addr:
    :return:
    '''

    answer = await send_message_recv_byte(message,addr)
    try:
        answer = pickle.loads(answer)
    except:
        # print("Pickle no pudo hacer nada con lo que le mandaron")
        return None
    # print('esto es lo que hizo pickle ',answer)
    return answer

async def send_message_recv_byte(message,addr):
    context = zmqa.Context()
    sock = context.socket(zmq.REQ)
    sock.connect("tcp://{}:{}".format(addr[0], addr[1]))
    sock.send_string(str(message))
    # print('este es el mensaje que le mando: ',message)
    answer = await sock.recv()
    # print('esto fue lo que me mandaron: ',answer)
    sock.close()
    return answer

async def send_message(message, addr):
    '''
    Este metodo manda el mensaje y espera que le manden un string por la red

    :param self:
    :param message:
    :param addr:
    :return:
    '''

    answer = await send_message_recv_str(message,addr)
    answer_message = Message().get_message(answer)
    # print('Esta fue la respuesta a mi mensaje: ',answer_message,' de: ',addr)
    return answer_message

async def send_message_recv_str(message, addr):
    '''
    Este metodo manda el mensaje y espera que le manden un string por la red

    :param self:
    :param message:
    :param addr:
    :return:
    '''
    context = zmqa.Context()
    sock = context.socket(zmq.REQ)
    # print('Tratemos de conectarnos a: ',addr)
    sock.connect("tcp://{}:{}".format(*addr))

    # print("Voy a mandar este mensaje desde sendmessage: ",message, ' a: ',addr)
    sock.send_string(str(message))
    answer = await sock.recv_string()
    # print('Esta fue la respuesta a mi mensaje: ',answer,' de: ',addr)
    sock.close()
    return answer


async def send_byte_data(data, addr):
    context = zmqa.Context()
    data_sender_socket = context.socket(zmq.REQ)
    data_sender_socket.connect('tcp://{}:{}'.format(*addr))

    data_sender_socket.send(data)
    message = await data_sender_socket.recv_string()
    # print("Recibimos el mensaje: ",message)
    data_sender_socket.close()
    return Message().get_message(message)

async def try_send_byte_data(send_data_func,data,addr,except_action,timeout=5):
    # print("this is the data im trying to send: ",data)
    answer = None
    try:
        data = pickle.dumps(data)
        answer = await asyncio.wait_for(send_data_func(data,addr),timeout=timeout)
    except asyncio.TimeoutError:
        except_action()
    return answer

def _send_ping_message_to_addr(addr):
    ping_socket = zmq.Context().socket(zmq.REQ)
    ping_socket.connect('tcp://{}:{}'.format(*addr))
    ping_socket.send_string(str(Message().set_message("PING")))
    try:
        answer = recv_message(ping_socket.recv_string,5)
    except TimeoutError:
        print("No me respondio el ping: ",addr)
        ping_socket.close()
        return -1
    print("Me respondieron el ping: ",answer)
    ping_socket.close()
    return 0


def bind_to_random_port(sock,ip='0.0.0.0'):
    while True:
        random_port = str(random.randrange(8080, 65534, 2))
        try:
            sock.bind('tcp://{}:{}'.format(ip, random_port))
            return sock, random_port
        except zmq.error.ZMQError:
            # print("Esta ocupado el puerto: ", random_port)
            continue


def loop_tool(f,*args):
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(f(*args))
    # loop.close()
    return result


def get_available_random_port(ip='0.0.0.0'):
    context = zmq.Context()
    sock_test = context.socket(zmq.REP)
    while True:
        random_port = str(random.randrange(8080, 65534, 2))
        try:
            sock_test.bind('tcp://{}:{}'.format(ip, random_port))
            sock_test.close()
            return random_port
        except zmq.error.ZMQError:
            # print("Esta ocupado el puerto: ", random_port)
            continue


def get_blocks(slices_urls, cnt_blocks):
    max_len = len(slices_urls)/cnt_blocks
    while True:
        result = []
        while len(result) < max_len:
            try:
                slice_url = slices_urls.pop()
                result.append(slice_url)
            except IndexError:
                if len(result) > 0:
                    yield result
                return 0
        yield result