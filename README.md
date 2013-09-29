stream_proxy
============

General proxy for simple streams of different transports, encoding and messagisation.

Aim is to support the following data flows.

Transport:
* UDP
* TCP
* Stdin/out
* Read and write to files
* Read and write to folders (ie one message per file)
* ZMQ message

Messagisation:
* Dgram (ie one message per packet)
* Length prefix
* Text Delimited

Encodings:
- None initially, but design should make it easy to plug these in.
