import json
import asyncio

names = [
    'DONE',
    'ERROR',
    'INPUT',
    'MSG',
    'OK',
    'OUTPUT',
    'PING',
    'TASK',
    'WORKER',
]


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
        try:
            messageSplit = message.split(" ", 2)
        except AttributeError:
            print(message)
            raise Exception("Y aqui voy a parar...")
        # print(messageSplit)
        name = messageSplit[0]
        payload_len = int(messageSplit[1])
        if payload_len > 0 and len(messageSplit) == 3:
            payload = json.loads(messageSplit[2])
        else:
            payload = {}

        return name, payload_len, payload

    def set_message(self, name, payload_dict={}):
        self.message_name = name
        self.payload = payload_dict
        self.payload_len = len(self.payload)
        return self

    @staticmethod
    def _group_list(message):
        b = ""
        for x in message:
            b += x
        return b

    def __str__(self):
        return "{} {} {}".format(self.message_name, self.payload_len, json.dumps(self.payload))


async def do_async(function_to_execute, time_to_wait, *args):
    # print("Parser:args:", *args)
    try:
        result = await asyncio.wait_for(function_to_execute(*args), time_to_wait)
        return result
    except asyncio.TimeoutError:
        print("Parser:Function:", function_to_execute.__name__, "timeout passed",
              time_to_wait, "seconds after started. Returning -1...")
        return -1
