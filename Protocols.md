# Protocolos para mensajes
**Input** y **Output** en lo siguiente se refiere a la entrada y salida de los métodos mediante los sockets destinados a esto.
**Return** se refiere al valor de retorno de las funciones respectivamente.
## PUTDATA
### Input:
`message_name`: `PUTDATA`

`payload`: `{"bytes": true_if_data_is_in_bytes}`
### Output:
A message with `OK` or `ERROR` on the `message_name` and in the case of the `ERROR` message, a "info" key on the payload describing the error
### Return:
El url del archivo guardado

### Sequence:

| User                                                         | FileSystem                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1- Sends a PUTDATA message with “file_url” and “bytes” fields | 2- Sends OK message with “ip” and “port” fields              |
| 3- Connects a new socket to the received “ip” and “port” and sends the data line by line. If it sends an empty line, the process stops | 4- Receives the lines one by one and writes them into the file located in “file_url” inside the FileSystem. When process stopped, sends an “OK” string. |
| 5- Closes the data socket                                    | 6- Closes the data socket                                    |



## PUTJOB
### Input
`message_name`: `PUTJOB`

`payload`: `{"job_hash": the_hash_assigned_to_the_job, "files": an_array_with_the_data_and_function_files}`

### Output
A message with `OK` or `ERROR` on the `message_name` and in the case of the `ERROR` message, a "info" key on the payload describing the error
### Return
El url del directorio de la tarea insertada

### Sequence

| User                                                         | FileSystem                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1- Sends a PUTJOB message with “job_id” and “files” fields. Where “files” is an array with the files’s names (str). | 2- Creates a directory with name “job_id”, creates and configures a sqlite3 database for the job, creates an empty `__init__.py` file and sends an “OK” message with “ip” and “port” to start sending the files declarated on the “files” field received |
| 3- Creates and connects a new socket with the received “ip” and “port”. Sends a “PUTDATA” message for every file declarated in “files”. Following starting the same PUTDATA procedure as for single files. | 4- Once all the files are received waits for a “STOP” message to continue, if this doesn’t happens it means that there are more data to send that the one is declared or a fault on the sync. |
| 5- Sends a “STOP” message through the data socket and waits for an “OK” message through the main socket | 6- Sends an “OK” with “job_url” and “files_url” fields where “job_url” is the directory where the job’s files are stored and “files_url” is an array with the urls on the FileSystem of the files sent (on the same order) including the sqlite3 database at the end of it. |



## GETDATA
### Input
`message_name`: `PUTJOB`

`payload`: `{"file_url": the_url_assigned_to_the_file, "bytes": true_if_data_is_in_bytes}`
### Output
A message with the same format as input message but adding an "data" key to the payload with the requested data
### Return
0 in case of correct execution, -1 otherwise

### Sequence

| User                                                         | FileSystem                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1- Sends a “GETDATA” message with “file_url” and “bytes” fields. | 2- If the data is available, sends an “OK” message with “ip” and “port” |
| 3- Creates and connects a new socket to “ip” and “port”. Send the same “GETDATA” message through this socket | 4- Sends line by line the data on string messages. If an empty string is sent, the process is done. |
| 5- Receives the data line by line and waits for an empty line to stop receiving. Then sends an “OK” string through the new socket. | 6- Receives the “OK” string and terminates.                  |



## REMOVEJOB
### Input
`message_name`: `REMOVEJOB`

`payload`: `{"file_url": the_url_assigned_to_the_file}`
### Output
A message with `OK` or `ERROR` on the `message_name` and in the case of the `ERROR` message, a "info" key on the payload describing the error
### Return
Nothing

### Sequence

| User                                                | FileSystem                                                   |
| --------------------------------------------------- | ------------------------------------------------------------ |
| 1- Sends a “REMOVEJOB” messaje with “job_url” field | 2- Removes every file on the “job_url” directory and then erases the folder. Sends an “OK” message if succeded. |

## INSERTJOB

### Input

`message_name`: `INSERTJOB`

`payload`: 

**First**

`db_url`

**Second**

`job_id` `tracker_ip_ping` `tracker_port_ping` `answer_ip` `answer_port` `status_phase` `map_data_url` `result_url` `job_state` `data_type`

### Output

An `OK` message

### Sequence

| User                                                         | FileSystem                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1- Sends an “INSERTJOB” message with “db_url” field          | 2- Opens the database in “db_url” and sends an “OK” messsage with “ip” and “port” fields |
| 3- Creates and connects a new socket and sends an “INSERTJOB” message with all the required fields | 4- Inserts the data into the “job” table inside the database and sends and “OK” message |

## INSERTBLOCK

### Input

`message_name`: `INSERTBLOCK`

`payload`:

**First**

`db_url`

**Second**

`block_id` `state` `phase` `slices_id`

### Output

An `OK` message

### Sequence

| User                                                         | FileSystem                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1- Sends an “INSERTBLOCK” message with “db_url” field        | 2- Sends an “OK” message with “ip” and “port” fields         |
| 3- Creates and connects a new socket to “ip” and “port” and sends the second “INSERTBLOCK” message with the required fields | 4- Sends an “OK” message through the new socket, closes the connection with the database and finishes |

## BLOCKRESULT

### Input

`message_name`: `BLOCKRESULT`

`payload`: 

**First**

`db_url`

**Second**

`block_id` `result_urls`

### Output

An “OK”message

### Sequence

| User                                                         | FileSystem                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1- Sends a “BLOCKRESULT” message with “db_url” field         | 2- Sends an “OK” message with “port” and “ip” fields         |
| 3- Creates and connects a new socket with “ip” and “port” and sends the “BLOCKRESULT” with the required fields | 4- Sends an “OK” message through the new socket, closes the connection with the database and returns |

## GETROWS

…

## UPDATEROW

…

## JOB
### Input
`message_name`: `JOB`

`payload`: `job_url job_id data_type client_addr`

### Output
An `OK` message

## WORKERREG
Para registrar un worker
### Input
`message_name`: `WORKERREG`

`payload`: `worker_addr ping_addr`
### Output
Un `OK` message

## JOBREG
Para registrar un trabajo
### Input
`message_name`: `JOBREG`

`payload`: `files addr`

### Output
`OK` message if really ok

## FILESYSREG
Para registrar al host del filesystem
### Input
`message_name`: `FILESYSREG`

`payload`: `addr`

### Output
Un `OK` message
## WORKERS
Para pedir una lista con los addr de mensaje y ping de todos los workers
### Input
`message_name`: `WORKERS`

`payload`: {}
 ### Output
 Un `OK` pero con un campo `workers` en el payload con la lista pedida