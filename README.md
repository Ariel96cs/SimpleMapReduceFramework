# notas

En el caso de que en el schedulling, no se pueda mandar tareas a ningun worker, la primera de ellas se hace en la 
máquina que corre el scheduler y al terminar se hace otra vuelta de schedulling.

**En el módulo `master`, función `_send_message` cuando se vaya a llamar a la función, usar multiprocessing con un 
timeout, el que termine antes de timeout, hace append en `working_workers` en caso de que responda afirmativamente**

El Scheduler hace un hash con los urls del archivo de función y el archivo de datos para darle **id** a la tarea

El Scheduler manda al worker un json con el addr del que esta esperando para copiarle los archivos de función y de datos

El cliente le pide al sistema de archivos que guarde un archivo y que le retorne el URL. El filesystem por ahora estará 
en el cliente

Estamos trabados en la parte en la que el cliente le manda los datos al filesystem. Queremos que lo mande de 1 vez y no 
tener que abrir el archivo, concatenar el string y despues mandarlo.

Pasamos el archivo con las funciones de map y reduce y los datos al file system y mandamos la dirección que tienen, 
igualmente se identifican con el hash.

En los workers mantendremos 2 procesos activos: uno escuchando y escribiendo en los sockets y otro trabajando en map o 
reduce

- En el ejemplo de:
    SELECT dog, AVG(age)
    FROM dog
    GROUP BY dog
    es parecido al ejemplo del counting_words.
    - Input: Lo que la entrda hay que modificarla un poco. los datos de la base de datos hay que llevarlo a un csv y
    eliminar la fila de los nombres de las tablas, pq sino la va a tratar como una fila mas.
    - Map: En el map hay que hacer split por salto de linea, para analizar linea por linea separado, y luego split por
    espacios, hay que saber de antemano cual es el indice de la columna que ocupa el dog. Devolver la tupla (dog,(age,1))
    el value es (la edad,la cantidad con esa edad).
    - Combiner: Sumar la lista de valores de cada llave, retornar (dog,(sum(values),cnt_sumandos))
    - Reducer: Sumar la lista de valores y retornar (dog,sum(values)/sum(cnt_sumandos))



todo sqlite:
- tenemos que :
    - insertar bloque: -> crear una tupla en la tabla bloque y crear las tuplas de
        los slices url y asociarle el id del bloque.
    - modificar el estado del bloque
    - asignar result url al bloque -> crear el result url si no existe y crear nueva tupla en la tabla de relacion
        entre bloque y result_url
    - insertar en la tabla job la tupla del trabajo.
    - modificar el job.
    -get_info

### Tablas
en la tabla de job vamos a poner los campos:
    'job_id' que es la llave,
  'tracker_ip_ping',
  'tracker_port_ping',
    'answer_ip','
    answer_port',
    'status_phase',
    'map_data_url',
    'result_url',
    'job_state',
    'data_type'

en la tabla block:
    la llave es el string '(phase)_(blocknum)',
    'state'->added, submitted, writing or done
    'phase'->map o reduce

en la tabla slices_url:
 la llave es la url 'slice_url'
 'block_id' que es una llave foranea
en la tabla result_url: temporalmente no existe

en la tabla block_result:
    lo que tenemos son dos campitos que son ambos llaves
    'block_id' y 'result_url'

Protocolos de comunicacion:
    -INSERTBLOCK payload{
    'status_db_url':la url del db,
    'block_id'(el string phase_blocknum),
    'state':addedmsubmitess,
    'slices_id':[lista de los url de los slices]
    'phase':map o reduce
    }
    -UPDATEROW payload{
    'status_db_url':la url del db,
    'table': la tabla,
     'key': la llave de la tabla,
     'fields_values':[(field_name,new_value)]}
    -BLOCKRESULT payload{
        'status_db_url':la url del db,
        'block_id': la llave del block,
        'result_urls':[la lista de las url de los resultados]
    }
    -INSERTJOB payload{
    'status_db_url':la url del db,
    'job_id' que es la llave:value,
    'tracker_ip_ping':value,
    'tracker_port_ping':value,
    'answer_ip':value,
    'answer_port':value,
    'status_phase':value,
    'map_data_url':value,
    'result_url':value,
    'job_state':value,
    'data_type':value
    }
    -GETROWS payload {
    'status_db_url':la url del db,
    'table': nombre de la tabla,
    'filters':[(field,value)]

    }
