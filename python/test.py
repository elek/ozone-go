#!/usr/bin/env python
from ctypes import *

lib = cdll.LoadLibrary("../lib/lib")

lib.CreateOmClient.argtypes = [c_char_p]
lib.CreateOmClient.restype = c_long
lib.PrintKey.argtypes = [c_long, c_char_p, c_char_p, c_char_p]

client = lib.CreateOmClient(b"localhost")
print(client)
lib.PrintKey(client, b"vol1", b"bucket1", b"key1")
