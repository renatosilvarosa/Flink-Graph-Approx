import socketserver
import sys
import time


class StreamRequestHandler(socketserver.BaseRequestHandler):
    file_name = ''
    interval = 1

    def handle(self):
        print("Accepted client at " + str(self.client_address))
        file = open(self.file_name)
        for line in file:
            self.request.send(line.encode())
            time.sleep(self.interval / 10)

        print("Closing client connection...")


def main():
    if len(sys.argv) < 3:
        print("Usage: stream_generator.py <file name> <stream interval>")
        sys.exit(-1)

    StreamRequestHandler.file_name = sys.argv[1]
    StreamRequestHandler.interval = int(sys.argv[2])

    server = socketserver.ThreadingTCPServer(("localhost", 1234), StreamRequestHandler)
    print("Listening...")
    server.serve_forever()


if __name__ == "__main__":
    main()
