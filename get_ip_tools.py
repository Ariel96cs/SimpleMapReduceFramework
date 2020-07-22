from netifaces import interfaces,ifaddresses,AF_INET

def get_ip():
    for iface in interfaces():
        if 'eth' in iface:
            return ifaddresses(iface).setdefault(AF_INET,[{'addr': '00'}])[0]['addr']
    return '127.0.0.1'