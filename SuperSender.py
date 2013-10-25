import sys
import getopt
import random
from collections import namedtuple
import os
import math
import threading

import Checksum
import BasicSender


BufferedData = namedtuple('BufferedData', ['acked', 'data'])

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

		#Window Size
		self.wind_size = 5

		#Buffering Packets To Send (Seq #, (inflight, data))
		self.buffer = dict()

		#Determining Number of Packets to Send
		file_size = os.fstat(self.infile.fileno()).st_size
		self.num_packets = int(math.ceil(float(file_size) / self.max_payload))

		#Timeout Timer
		self.timers = dict()
		self.lock = threading.Lock()

	# Main sending loop.
	def start(self):
		#print "===== Welcome to Bears-TP Sender v1.0! ====="
		self.isn = random.randint(0, 100)

		if (self._initialize_connection(self.retry_count)):
			
			#Sent first window of packets
			self._initialize_and_send_buffer()

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
		#Remove all items in buffer less than the base_num. For cleanup purposes
		for seqno in self.buffer.keys():
			if seqno < self.send_base:
				del self.buffer[seqno]

		#Add up to WIND_SIZE packets into the buffer
		for i in range(self.wind_size):
			seqno = self.send_base + i
			#We already have this packet in the buffer
			if self.buffer.has_key(seqno):
				pass
			else:
				data = self.infile.read(self.max_payload)
				if data:
					#We have Data!
					self.buffer[seqno] = BufferedData(False, data)

					#Send this data right away!
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
						#print "ACK: %d" % (ack - self.isn)
						if ack >= self.send_base and ack < self.send_base + self.wind_size:
							#Ack is for packet within our window. Set acked as TRUE in our buffer
							self.buffer[ack] = BufferedData(True, self.buffer[ack].data)
							self._set_send_base()
							self._timer_stop(ack)
							if self.send_base - self.isn > self.num_packets:
								#We are DONE! Our send_base is higher than the number of packets we are to send
								return
							else:
								#Buffer more packets and send them
								self._initialize_and_send_buffer()

				else:
					#Not good packet! Listen for acks again
					pass

	def _timeout(self, seqno):
		#Timeout Function. Just resubmit the send_base packet and reset the timer
		self._transmit(seqno)
		if not self.send_base - self.isn > self.num_packets:
			self._timer_restart(seqno)

	def _transmit(self, seqno):
		#Send a single packet.
		msg_type = "data"
		if self.buffer.has_key(seqno):
			buffered_data = self.buffer[seqno]

			#print "TRANSMITED: %d" % (seqno - self.isn)
			packet = self.make_packet(msg_type, seqno, buffered_data.data)
			self.send(packet)
			self._timer_restart(seqno)

	def _timer_stop(self, seqno):
		#Stops the timeout timer
		with self.lock:
			if self.timers.has_key(seqno):
				self.timers[seqno].cancel()
				del self.timers[seqno]

	def _timer_restart(self, seqno):
		with self.lock:
			if self.timers.has_key(seqno):
				self.timers[seqno].cancel()
				del self.timers[seqno]
			self.timers[seqno] = threading.Timer(self.timeout, self._timeout, [seqno])
			self.timers[seqno].start()

	def _set_send_base(self):
		sorted_buffer = sorted(self.buffer.items())
		for seqno, buffered_data in sorted_buffer:
			if buffered_data.acked:
				self.send_base += 1
			else:
				break

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
					self.send_base = self.isn + 1
					if msg_type == "ack":
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
					if seqno == self.send_base and msg_type == "ack":
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
