from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM
import random
import hashlib
from threading import Thread
from time import sleep, time
import json
from typing import Dict

DATAGRAM_ACK = 0
DATAGRAM_NORMAL = 1
DATAGRAM_RELIABLE = 2
MAX_RESEND_TRIES = 3
BUFFER_SIZE = 4096
TIME_FOR_RESEND = 0.5

class NetworkAgent(object):

    def __init__(self,response_handler, net_address='localhost', port=10000):
        self.server = socket(AF_INET, SOCK_DGRAM)
        self.server.bind((net_address, port))
        self.packetManager = PacketManager(BUFFER_SIZE)
        self.response_handler = response_handler
        self.net_address = net_address
        self.port = port
        self.packetsWaitingAck = {}

    def start(self):
        print('Starting server on  {}:{}'.format(self.net_address, self.port))
        listenLoop = Thread(target=self.__listenLoop)
        resend_lost_packets_thread =  Thread(target=self.__resendLostPackets)
        listenLoop.start()
        resend_lost_packets_thread.start()




    def __listenLoop(self): #TODO duda si se queda esperando a mensajes que nunca llegaran?
        packet = Packet()
        complete_received_messages = []

        receivedPackets = {}

        print("Starting listen loop")
        while 1:

            data, sender_address = self.server.recvfrom(BUFFER_SIZE)

            if self.__getPacketType(data) == DATAGRAM_ACK:
                print('Ack received:')
                ackPacket = AckPacket()
                ackPacket.byteContent = data
                ackPacket.printInfo()
                message_id = ackPacket.getMessageId()
                packet_number = ackPacket.getPacketNumber()
                del (self.packetsWaitingAck[message_id])[packet_number]

            else:

                packet.byteContent = data
                packet_content = packet.getContent()

                # print('Data received:')
                # packet.printInfo()


                if packet.isCorrect() and packet_content['type'] == DATAGRAM_RELIABLE:
                    ackPacket = AckPacket(packet)
                    if ackPacket.getPacketNumber() != 5:
                        # print(f'Sending ACK to {sender_address}')
                        self.server.sendto(ackPacket.getBytes(), sender_address)
                        print()

                #If it´s a new message, inicialize receiving list
                if packet_content['message_id'] not in receivedPackets.keys():
                    receivedPackets[packet_content['message_id']] = [None for _ in range(packet_content["total_packets"])]

                #Save packet payload in the corresponding position of his list
                receivedPackets[packet_content['message_id']][packet_content['packet_number']] = packet_content['payload']

                for message_id, packets in receivedPackets.items():
                    #Check if message is complete
                    if None not in packets:
                        self.response_handler(self.__list_to_bytes(packets), sender_address)
                        complete_received_messages.append(message_id)

                #Delete complete messages from receiving dictionary
                for message_id in complete_received_messages:
                    del receivedPackets[message_id]

                complete_received_messages = []


    def __list_to_bytes(self, packets):
        out = (b''.join([msg for msg in packets]))
        print(out)
        return out



    def send(self, message, destination, reliable=DATAGRAM_RELIABLE):

        packetSet = self.packetManager.createPacketSet(message, reliable)

        messageId = packetSet[0].getMessageId()
        self.packetsWaitingAck[messageId] = {}

        for packet in packetSet:
            self.server.sendto(packet.getBytes(),destination)

            if reliable is DATAGRAM_RELIABLE:
                sent_packet_info = {}
                sent_packet_info['packet'] = packet
                sent_packet_info['destination'] = destination
                sent_packet_info['sent_time'] = time()
                sent_packet_info['remaining_attempts'] = MAX_RESEND_TRIES

                #Classify sent packets by message_id to avoid resending packets of a message with any packet
                #that has exeeded the maximum send retries (why having an incomplete message?)
                self.packetsWaitingAck[messageId][packet.getPacketNumber()] = sent_packet_info

    def send_json(self, json_to_send, data_type, to):
        """
        Envía un json a través del socket
        :param json_to_send: JSON a enviar
        :param data_type: Tipo de datos a enviar, ACK, NORMAL o RELIABLE
        :param to: Cliente al que vamos a enviar los datos
        """
        json_bytes = json.dumps(json_to_send).encode(encoding='utf-8')
        self.send(json_bytes,to,data_type)
        return



    def __resendLostPackets(self):
        to_give_up_messages = []
        print("Starting resend loop")
        while 1:
            sleep(0.5)
            for message_id, sent_packet_info_dict in self.packetsWaitingAck.items():
                for sent_packet_info in sent_packet_info_dict.values():

                    if sent_packet_info['remaining_attempts'] == 0:
                        print(f"Packet {sent_packet_info['packet'].getPacketNumber()} has exceeded the maximum attempts, giving up on message {message_id}")
                        to_give_up_messages.append(message_id)
                        break

                    time_sience_last_attempt = time() - sent_packet_info['sent_time']

                    if(time_sience_last_attempt > TIME_FOR_RESEND):
                        print(f"Resending packet {sent_packet_info['packet'].getPacketNumber()} to {sent_packet_info['destination']}")
                        self.server.sendto(sent_packet_info['packet'].getBytes(),sent_packet_info['destination'])
                        sent_packet_info['sent_time'] = time()
                        sent_packet_info['remaining_attempts'] -= 1

            for message_id in to_give_up_messages:
                del self.packetsWaitingAck[message_id]

            to_give_up_messages = []



    def __getPacketType(self, data):
        return int.from_bytes(data[:4], 'little')






####################################################################################
#########################                               ############################
#########################         AUXILIAR CLASSES      ############################
#########################                               ############################
####################################################################################




class PacketManager(object):
    """
    Performs packet set creation from message and handles message_id
    """
    def __init__(self,buffer_size=2048):
        self.buffer_size = buffer_size
        self.message_id = random.randint(0, 1000000)

    def createPacketSet(self,message,type):
        msgs = self._split(message, self.buffer_size)
        packet_set = []
        print("Creating set")


        for i, chunk in enumerate(msgs):
            #chunk = len(msgs).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
            packet = Packet(type,self.message_id,len(msgs),i,chunk)

            packet_set.append(packet)
            print(f'Packet data: {packet.getContent()["payload"]}')

        self.message_id += 1
        return packet_set


    def _split(self,b, size):
        result = []
        while len(b) > size:
            result.append(b[:size])
            b = b[size:]
        result.append(b)
        return result







class Packet(object):
    """
       Performs packet set creation from message and handles message_id
    """
    def __init__(self, type=None, message_id=None, total_packets=None, packet_number=None, payload=None):
        if type is not None:
            hasher = hashlib.sha256()
            hasher.update(payload)
            self.byteContent = type.to_bytes(4, 'little')
            self.byteContent += message_id.to_bytes(4, 'little')
            self.byteContent += hasher.digest()
            self.byteContent += total_packets.to_bytes(4, 'little')
            self.byteContent += packet_number.to_bytes(4, 'little')
            self.byteContent += payload
            #TODO comprobar que tipo es content para ver si hay que pasar a bytes


    def getBytes(self):
        return self.byteContent

    def getContent(self):
        content = {}
        content['type'] = int.from_bytes(self.byteContent[:4], 'little')
        content['message_id'] = int.from_bytes(self.byteContent[4:8], 'little')
        content['hash_sum'] = self.byteContent[8:40]
        content['total_packets'] = int.from_bytes(self.byteContent[40:44], 'little')
        content['packet_number'] = int.from_bytes(self.byteContent[44:48], 'little')
        content['payload'] = self.byteContent[48:]
        return content

    def isCorrect(self):
        hasher = hashlib.sha256()
        packageData = self.getContent()
        hasher.update(packageData['payload'])

        originalPacketSum = packageData['hash_sum']
        actualPacketSum = hasher.digest()

        return originalPacketSum == actualPacketSum

    def getMessageId(self):
        return int.from_bytes(self.byteContent[4:8], 'little')

    def getPacketNumber(self):
        return int.from_bytes(self.byteContent[44:48], 'little')




    def printInfo(self):
        packet_content = self.getContent()
        print(f'type -> {packet_content["type"]}')
        print(f'message_id -> {packet_content["message_id"]}')
        print(f'total_packets -> {packet_content["total_packets"]}')
        print(f'hash_sum -> {packet_content["hash_sum"]}')
        print(f'packet_number -> {packet_content["packet_number"]}')
        #print(f'content -> {packet_content["payload"]}')
        print(f'check sum -> {self.isCorrect()}')










class AckPacket(object):
    def __init__(self,packet=None):
        if packet is not None:
            original_packet_bytes = packet.getBytes()
            self.byteContent = (DATAGRAM_ACK).to_bytes(4, 'little')
            self.byteContent += original_packet_bytes[4:8]
            self.byteContent += original_packet_bytes[44:48]

    def getBytes(self):
        return self.byteContent

    def getContent(self):
        content = {}
        content['type'] = int.from_bytes(self.byteContent[:4], 'little')
        content['message_id'] = int.from_bytes(self.byteContent[4:8], 'little')
        content['packet_number'] = int.from_bytes(self.byteContent[8:], 'little')
        return content

    def getMessageId(self):
        return int.from_bytes(self.byteContent[4:8], 'little')

    def getPacketNumber(self):
        return int.from_bytes(self.byteContent[8:], 'little')

    def printInfo(self):
        packet_content = self.getContent()
        print(f'type -> {packet_content["type"]}')
        print(f'message_id -> {packet_content["message_id"]}')
        print(f'packet_number -> {packet_content["packet_number"]}')
        print()






