# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: message.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rmessage.proto\x12\x13\x46unctionComposotion\"?\n\x0bRequestInfo\x12\x0c\n\x04step\x18\x01 \x01(\x05\x12\x10\n\x08\x44\x41G_name\x18\x02 \x01(\t\x12\x10\n\x08parallel\x18\x03 \x01(\x05\"`\n\tReplyInfo\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x35\n\x0cworkers_perf\x18\x02 \x03(\x0b\x32\x1f.FunctionComposotion.WorkerPerf\x12\x0b\n\x03\x63\x61p\x18\x03 \x01(\x02\"A\n\nWorkerPerf\x12\x16\n\tworker_id\x18\x01 \x01(\x05H\x00\x88\x01\x01\x12\r\n\x05perfs\x18\x02 \x03(\x02\x42\x0c\n\n_worker_id2[\n\x08NodeComm\x12O\n\tFunc_Exec\x12 .FunctionComposotion.RequestInfo\x1a\x1e.FunctionComposotion.ReplyInfo\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'message_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_REQUESTINFO']._serialized_start=38
  _globals['_REQUESTINFO']._serialized_end=101
  _globals['_REPLYINFO']._serialized_start=103
  _globals['_REPLYINFO']._serialized_end=199
  _globals['_WORKERPERF']._serialized_start=201
  _globals['_WORKERPERF']._serialized_end=266
  _globals['_NODECOMM']._serialized_start=268
  _globals['_NODECOMM']._serialized_end=359
# @@protoc_insertion_point(module_scope)
