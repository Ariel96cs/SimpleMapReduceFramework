import register

r = register.Register(ip='10.0.0.1',port='9091')
r.start_server()
print("Out of the server?!")
