import sys
import getopt
from collections import namedtuple
import os
import math
import threading

import Checksum
import BasicSender

'''
This is a skeleton sender class. Create a fantastic transport protocol here.
'''
class Sender(BasicSender.BasicSender):
	def __init__(self, dest, port, filename, debug=False):
		super(Sender, self).__init__(dest, port, filename, debug)

		#Number of times to retry connection. Make it very large so we keep bothering receiver 5ever before it talks to us. Don't ignore us bitch
		self.retry_count = 10000000000000

		# 0.5 Second timeout (500ms)
		self.timeout = 0.5

		#Max Payload
		self.max_payload = 1472

		# CC: Changing window size; initially set to CWND = 1
		self.wind_size = 1

		# CC: Some settings
		self.phase = "SLOW START"
		self.r_wind_size = 5 # Receiver window size; not necessarily the same as RWND but only modifying sender so self.r_wind_size is substitute
		self.SSTHRESH = 64000 # TODO: How to set this value initially
		self.CWND = 1

		#Buffering Packets To Send (Seq #, (inflight, data))
		self.buffer = dict()

		#Determining Number of Packets to Send
		file_size = os.fstat(self.infile.fileno()).st_size
		self.num_packets = math.ceil(float(file_size) / self.max_payload)
		#print self.num_packets

		#Timeout Timer
		self.timer = None
		self.lock = threading.Lock()

		#For DUP ACKS
		self.prev_ack = None
		self.dup_ack_count = 0
		self.dup_ack_max = 3

	# Main sending loop.
	def start(self):
		#print "===== Welcome to Bears-TP Sender v1.0! ====="
		self.isn = 0

        # self.initialize_connection initializes the connection
        # and self.send_base, which maintains the invariant that
        # it is always the seqno of the leftmost packet in window
		if (self._initialize_connection(self.retry_count)):

			#Edge case where we have no data to send
			if self.num_packets > 0:
				#Send first window of packets
				self._initialize_and_send_buffer()

				#Begin Timeout Timer
				self._timer_restart()

				#Start listening for acks and advance sliding window as needed
				self._listen_for_acks()

			#Done sending everything!
			if self._tear_down_connection(self.retry_count):
				pass
			else:
				#print "Could not tear down connection. Will just assume it is gone 5ever"
				pass
		else:
			#print "Could not connect to the receiving socket"
			pass

	def _initialize_and_send_buffer(self):
		#Remove all items in buffer less than self.send_base.
		#Dictionary maintains the invariant that it contains packets that we processed at least once.
		#self.buffer does not necessarily only contain packets that are currently in our window.
        # For cleanup purposes
		for seqno in self.buffer.keys():
			if seqno < self.send_base:
				del self.buffer[seqno]

		#Add up to WIND_SIZE packets into the buffer
		for i in range(self.wind_size): # i = 0,1,2,3,4, ...
			seqno = self.send_base + i
			if self.buffer.has_key(seqno):
                #We already have this packet in the buffer
				pass
			else:
				data = self.infile.read(self.max_payload)
				if data:
					#We have data!
					self.buffer[seqno] = data

					#We only transmit new data
					self._transmit(seqno)
				else:
					#We ran out of data
					break

	def _listen_for_acks(self):
		while True:
			#Always listen for acks
			response = self.receive()

			if response:
				if Checksum.validate_checksum(response):
						#Good Packet!
						msg_type, seqno, data, checksum = self.split_packet(response)
						if msg_type != "ack":
							continue
						ack = int(seqno)
						if ack < self.send_base:
							continue
						if self.handle_new_ack(ack):
							return
				else:
					#Not good packet! Listen for acks again
					pass

	def _transmit(self, seqno):
		#Send a single packet.
		msg_type = "data"
		if self.buffer.has_key(seqno):
			data = self.buffer[seqno]

			#print "TRANSMITED: %d" % (seqno - self.isn)
			packet = self.make_packet(msg_type, seqno, data)
			self.send(packet)

	def _timer_stop(self):
		#Stops the timeout timer
		with self.lock:
			if self.timer:
				self.timer.cancel()
				self.timer = None


	def _timer_restart(self):
		with self.lock:
			if self.timer:
				self.timer.cancel()
				self.timer = None
			self.timer = threading.Timer(self.timeout, self.handle_timeout)
			self.timer.start()

	def _initialize_connection(self, retry_count):
		#print "WHY HELLO THERE"
		#Three Way Handshake
		if retry_count > 0:

			#Fields of the packet
			msg_type = 'start'
			msg = ""

			#Create and send the initlization packet
			start_packet = self.make_packet(msg_type, self.isn, msg)
			self.send(start_packet)

			#Wait 500ms to receive the packet
			response = self.receive(timeout=self.timeout)

			if response:
				if Checksum.validate_checksum(response):
					#Good Packet!
					msg_type, seqno, data, checksum = self.split_packet(response)
					ack = int(seqno)
					if msg_type == "ack" and ack == self.isn + 1:
						self.send_base = ack
						return True
					else:
						return self._initialize_connection(retry_count - 1)
				else:
					#Not good packet!
					return self._initialize_connection(retry_count - 1)
			else:
				#Timeout
				return self._initialize_connection(retry_count - 1) 
		else:
			#Could not connect
			return False      

	def _tear_down_connection(self, retry_count):
		#print "TEAR DOWN THIS WALL"
		if retry_count > 0:

			#Fields of the packet
			msg_type = 'end'
			msg = ""

			#Create and send the tear down packet
			tear_down_packet = self.make_packet(msg_type, self.send_base, msg)
			self.send(tear_down_packet)

			#Wait 500ms to receive the packet
			response = self.receive(timeout=self.timeout)

			if response:
				if Checksum.validate_checksum(response):
					#Good Packet!
					msg_type, seqno, data, checksum = self.split_packet(response)
					seqno = int(seqno)
					if seqno >= self.send_base + 1 and msg_type == "ack":
						return True
					else:
						#Wrong SEQ NO EXPECTED
						return self._tear_down_connection(retry_count - 1)
				else:
					#Not good packet!
					return self._tear_down_connection(retry_count - 1)
			else:
				#Timeout
				return self._tear_down_connection(retry_count - 1) 
		else:
			#Could not tear down packet. Should we just stop sending messages?
			return False     


	def handle_timeout(self):
		# CC: Timeout Function. Just resubmit missing segment
		self.cc_handle_timeout()
		self._transmit(self.send_base)
		if not self.send_base - self.isn > self.num_packets:
			self._timer_restart()

	def cc_handle_timeout(self):
		self.SSTHRESH = self.CWND/2
		self.CWND = 1
		self.wind_size = min(self.r_wind_size, int(math.floor(self.CWND)))
		self.phase = "SLOW START"

	def handle_new_ack(self, ack):
		#Returns True if we are done sending file, False otherwise
		#print "ACK: %d" % (ack - self.isn)

		#Dup Acks
		if self.prev_ack is not None and self.prev_ack == ack:
			self.dup_ack_count += 1
			if self.dup_ack_count >= self.dup_ack_max:
				self.handle_dup_ack(ack)
		else:
			self.dup_ack_count = 0

		self.prev_ack = ack

		if ack > self.send_base:
			#We can move our window forward! And stop our timer!
			self.cc_handle_new_ack()
			self._timer_stop()
			self.send_base = ack
			if self.send_base - self.isn > self.num_packets:
				#We received an ack for a packet seq no. greater than the num of packets we need to send. We are DONE!
				return True
			else:
				#Buffer more packets and send them
				self._initialize_and_send_buffer()

				#Start timer again!
				self._timer_restart()
		return False

	def cc_handle_new_ack(self):
		if (self.phase == "SLOW START"): # CC: Handle a new ACK in Slow Start
			self.CWND = self.CWND + 1
			self.wind_size = min(self.r_wind_size, int(math.floor(self.CWND)))
			if(self.CWND >= self.SSTHRESH):
				self.phase = "CONGESTION AVOIDANCE"
		elif (self.phase == "FAST RECOVERY"):
			self.CWND = self.SSTHRESH
			self.wind_size = min(self.r_wind_size, int(math.floor(self.CWND)))
			self.phase = "CONGESTION AVOIDANCE"
		else: # CC: self.phase == "CONGESTION AVOIDANCE"
			self.CWND = self.CWND + 1/(math.floor(self.CWND))
			self.wind_size = min(self.r_wind_size, int(math.floor(self.CWND)))

	def handle_dup_ack(self, ack):
		#print "DUP ACK: %d" % (ack - self.isn)
		self.cc_handle_dup_ack()
		if self.buffer.has_key(ack):
			self._transmit(ack)

	def cc_handle_dup_ack(self):
		if (self.dup_ack_count == 3):
			self.SSTHRESH = self.CWND/2
			self.CWND = self.SSTHRESH + 3
			self.wind_size = min(self.r_wind_size, int(math.floor(self.CWND)))
			self.phase = "FAST RECOVERY"
		else: # self.dup_ack_count > 3, already in "FAST RECOVERY" phase
			self.CWND = self.CWND + 1
			self.wind_size = min(self.r_wind_size, int(math.floor(self.CWND)))

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
