#!/usr/bin/env python
from ctypes import *

lib = cdll.LoadLibrary("./ozone.so")

lib.CreateOmClient.argtypes = [c_char_p]
lib.CreateOmClient.restype = c_long
lib.PrintKey.argtypes = [c_long, c_char_p, c_char_p, c_char_p]

client = lib.CreateOmClient(b"192.168.112.5")
print(client)
lib.PrintKey(client, b"vol1", b"bucket1", b"file1")
