# Copyright (c) 2020-present haozi3156666, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import struct


class Connector:
    sock = None
    packet_len = 4

    def connect(self, host, port, timeout=10):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(timeout)
            self.sock.connect((host, port))
            self.sock.settimeout(None)
        except socket.error as e:
            print('Connect to server error: %s' % e)
            self.sock.close()

    def disconnect(self):
        self.sock.close()

    def read(self, length):
        recv = b''
        while True:
            buf = self.sock.recv(length)
            if not buf:
                raise Exception('TSocket: Could not read bytes from server')
            read_len = len(buf)
            if read_len < length:
                recv = recv + buf
                length = length - read_len
            else:
                return recv + buf

    def write(self, buf):
        self.sock.sendall(buf)

    def read_next_packet(self):
        data = self.read(self.packet_len)
        data_len = struct.unpack('>i', data)
        return self.read(data_len[0])

    def write_with_header(self, data):
        self.write(struct.pack('>i', len(data)))
        self.write(data)
