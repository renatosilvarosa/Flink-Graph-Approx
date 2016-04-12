#! /usr/bin/python3

import socketserver
import sys
import time
from datetime import datetime

edge_path = sys.argv[1]
dates_path = sys.argv[2]

citations = {}
publications = {}

with open(edge_path) as edge_file:
    for l in edge_file:
        if not l.startswith("#"):
            vv = l.strip().split("\t")
            citations.setdefault(vv[0], list()).append(vv[1])

with open(dates_path) as dates_file:
    for l in dates_file:
        if not l.startswith("#"):
            dd = l.strip().split("\t")
            publications.setdefault(datetime.strptime(dd[1], "%Y-%m-%d").toordinal(), list()).append(dd[0])

start_date = min(publications.keys())
end_date = max(publications.keys())


class CitationStreamHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print("Accepted client at " + str(self.client_address))

        for day in range(start_date, end_date + 1):
            for paper in publications.get(day, list()):
                for c in citations.get(paper, list()):
                    msg = "{0},{1}\n".format(paper, c)
                    self.request.send(msg.encode())
            time.sleep(0.05)

        print("Closing client connection...")


server = socketserver.ThreadingTCPServer(("localhost", 1234), CitationStreamHandler)
print("Listening...")
server.serve_forever()
