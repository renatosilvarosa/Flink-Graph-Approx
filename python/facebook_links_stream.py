#! /usr/bin/python3

import socketserver
import sys
import time
from datetime import datetime

edge_path = sys.argv[1]
links = dict()

with open(edge_path) as edge_file:
    for l in edge_file:
        if not l.startswith("#"):
            vv = l.strip().split("\t")
            links.setdefault(int(vv[2]), list()).append((vv[0], vv[1]))

start_date = min(links.keys())
end_date = max(links.keys())


class FacebookLinksStreamHandler(socketserver.BaseRequestHandler):
    queries = 0

    def handle(self):
        print("Accepted client at " + str(self.client_address))

        i = 0
        for instant in range(start_date, end_date + 1):
            if instant in links:
                for edge in links[instant]:
                    msg = "A {0} {1}\n".format(edge[0], edge[1])
                    self.request.send(msg.encode())

            i += 1
            if i >= 604800:
                self.query(instant)
                time.sleep(20)
                i = 0

        self.query(end_date)
        self.request.send("END".encode())

        print("Closed client connection.")

    def query(self, instant):
        self.queries += 1
        date_str = datetime.fromtimestamp(instant).strftime("%Y-%m-%d")
        msg = "Q " + date_str + "\n"
        print("Query", str(self.queries), ":", msg, end='')
        self.request.send(msg.encode())


server = socketserver.ThreadingTCPServer(("localhost", 2345), FacebookLinksStreamHandler)
print("Listening...")
server.serve_forever()
