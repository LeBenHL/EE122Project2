import sys
import getopt
import random

import Checksum
import BasicSender

'''
This is a skeleton sender class. Create a fantastic transport protocol here.
'''
class Sender(BasicSender.BasicSender):
    def __init__(self, dest, port, filename, debug=False):
        super(Sender, self).__init__(dest, port, filename, debug)

        #Number of times to retry connection
        self.retry_count = 3

        # 0.5 Second timeout (500ms)
        self.timeout = 0.5

        #Max Payload
        self.max_payload = 1472

    # Main sending loop.
    def start(self):
        print "===== Welcome to Bears-TP Sender v1.0! ====="
        #Choose a random start sequence
        self.sequence_number = random.randint(0, 100)
        
        if(self.initialize_connection(self.retry_count)):
            payload = self.infile.read(self.max_payload)
            while payload:
                print payload
                #ITERATION 1 STOP AND GO
                msg_type = "data"

                #Send a single packet
                packet = self.make_packet(msg_type, self.sequence_number, payload)
                self.send(packet)

                #Wait 500ms to receive the packet
                response = self.receive(timeout=self.timeout)

                if response:
                    if Checksum.validate_checksum(response):
                        #Good Packet!
                        msg_type, seqno, data, checksum = self.split_packet(response)
                        self.sequence_number = int(seqno)
                        payload = self.infile.read(self.max_payload)
                    else:
                        #Not good packet!
                        pass

                else:
                    #Timeout send the same payload in the loop
                    pass

            #Done sending everything!
            if self.tear_down_connection(self.retry_count):
                pass
            else:
                print "Could not tear down connection. Will just assume it is gone 5ever"
        else:
            print "Could not connect to the receiving socket"

    def initialize_connection(self, retry_count):
        if retry_count > 0:

            #Fields of the packet
            msg_type = 'start'
            msg = ""

            #Create and send the initlization packet
            start_packet = self.make_packet(msg_type, self.sequence_number, msg)
            self.send(start_packet)

            #Wait 500ms to receive the packet
            response = self.receive(timeout=self.timeout)

            if response:
                if Checksum.validate_checksum(response):
                    #Good Packet!
                    msg_type, seqno, data, checksum = self.split_packet(response)
                    self.sequence_number = int(seqno)
                    return True
                else:
                    #Not good packet!
                    return self.initialize_connection(retry_count - 1)
            else:
                #Timeout
                return self.initialize_connection(retry_count - 1) 
        else:
            #Could not connect
            return False      

    def tear_down_connection(self, retry_count):
        if retry_count > 0:

            #Fields of the packet
            msg_type = 'end'
            msg = ""

            #Create and send the tear down packet
            start_packet = self.make_packet(msg_type, self.sequence_number, msg)
            self.send(start_packet)

            #Wait 500ms to receive the packet
            response = self.receive(timeout=self.timeout)

            if response:
                if Checksum.validate_checksum(response):
                    #Good Packet!
                    return True
                else:
                    #Not good packet!
                    return self.tear_down_connection(retry_count - 1)
            else:
                #Timeout
                return self.tear_down_connection(retry_count - 1) 
        else:
            #Could not tear down packet. Should we just stop sending messages?
            return False     


    def handle_timeout(self):
        pass

    def handle_new_ack(self, ack):
        pass

    def handle_dup_ack(self, ack):
        pass

    def log(self, msg):
        if self.debug:
            print msg

'''
This will be run if you run this script from the command line. You should not
change any of this; the grader may rely on the behavior here to test your
submission.
'''
if __name__ == "__main__":
    def usage():
        print "BEARS-TP Sender"
        print "-f FILE | --file=FILE The file to transfer; if empty reads from STDIN"
        print "-p PORT | --port=PORT The destination port, defaults to 33122"
        print "-a ADDRESS | --address=ADDRESS The receiver address or hostname, defaults to localhost"
        print "-d | --debug Print debug messages"
        print "-h | --help Print this usage message"

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                               "f:p:a:d", ["file=", "port=", "address=", "debug="])
    except:
        usage()
        exit()

    port = 33122
    dest = "localhost"
    filename = None
    debug = False

    for o,a in opts:
        if o in ("-f", "--file="):
            filename = a
        elif o in ("-p", "--port="):
            port = int(a)
        elif o in ("-a", "--address="):
            dest = a
        elif o in ("-d", "--debug="):
            debug = True

    s = Sender(dest,port,filename,debug)
    try:
        s.start()
    except (KeyboardInterrupt, SystemExit):
        exit()
