import message_tools as mt
import time

class ProgressTaskIterator:
    def __init__(self, job_id, task, status_handler, file_sys_addr):
        '''

        Tengo que poner a esperar a que inserten en la tabla block de status_db_url los bloques correspodientes
        a la tarea de task.
        lo que tengo que esperar que el status_phase sea distinto de 'GETWORKERS' y 'SLICES'
        esto quiere decir que que ya estan puestos todos los bloques, y los cargo e inicializo self.blocks_id
        ESTE PROGRESSTASK HAY QUE MANDARLO A CORRER JUSTAMENTE DESPUES QUE MANDE EL MENSAJE DE JOB,
        EN UN HILO APARTE.

        :param job_id:
        :param task:
        '''

        self.job_id = job_id
        self.task = task
        self.status_handler = status_handler
        self.file_sys_addr = file_sys_addr



        # todo: guardar los bloques id aqui
        filters = [('job_id', job_id)]
        jobs = self.status_handler.get_status_rows(file_sys_addr, 'job', filters)

        while True:
            if len(jobs) > 0:

                # si ya pasamos de map a reduce

                # if jobs[0]['job_state'] != self.task:
                #     break
                if jobs[0]['job_state'] > self.task:
                    break

                # si ya pasamos las fases de gettingworkers y slices, implica que ya se pusieron
                # todos los bloques de la tarea actual

                elif jobs[0]['status_phase'] != mt.task_phases[0] \
                        and jobs[0]['status_phase'] != mt.task_phases[1]\
                        and jobs[0]['job_state'] == self.task:
                    break
            jobs = self.status_handler.get_status_rows(file_sys_addr, 'job', filters)
            time.sleep(0.5)
        # print("Progress: current job status: ",jobs[0])
        # Ya aqui estamos en un estado donde tenemos todos los bloques de la tarea que esperamos mostrar el progreso
        print("Progress: Comenzando la tarea: ",self.task)
        filters = [('phase', self.task)]
        blocks_rows = self.status_handler.get_status_rows(file_sys_addr, 'block', filters)
        self.blocks_id = [block_row['block_id'] for block_row in blocks_rows]
        self.len_blocks = len(self.blocks_id)
        self.done_blocks = []
        self.yielded_blocks = 0

    def __len__(self):
        return self.len_blocks

    def __iter__(self):
        '''

        :return:
        '''

        while self.yielded_blocks < self.len_blocks:
            for block_id in self.done_blocks:
                # print("Progress:  vamos a hacerle yield a: ",block_id)
                yield block_id
                self.yielded_blocks += 1
            self.done_blocks = []
            # me quedo con los bloques que esten corriendo mi tareaself.task y que esten en DONE

            filters = [('phase', self.task),('state', mt.task_phases[-1])]
            blocks = self.status_handler.get_status_rows(self.file_sys_addr, 'block', filters)
            # print("Progress: Estos son los bloques que me mandaron: ",blocks)
            done_blocks = [block['block_id'] for block in blocks if block['block_id'] in self.blocks_id]
            blocks_id = [block_id for block_id in self.blocks_id if block_id not in done_blocks]
            self.blocks_id = blocks_id
            self.done_blocks = done_blocks
            time.sleep(0.5)
        # print("Progress: Ended task: ",self.task)
