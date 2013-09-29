#!/usr/bin/ruby

##### Interfaces #####

class EndOfTransport < IOError
  """ Exception indicating the end of the data stream. """
end

class InvalidData < RuntimeError
  """ Exception indicating data cannot be encoded or decoded """
end

class Receiver
    def recv
        """ Return the next message."""
        raise NotImplementedError
    end
end

class Encoder
    def encode(message)
        """ Return the encoded message """
        raise NotImplementedError
    end

    def decode(data)
        """ Return (message, remaining_data)"""
        raise NotImplementedError
    end
end

class Sender
    def send(message)
        """ Send the message """
        raise NotImplementedError
    end
end

###### Transport options #####

class UdpReceiver
    """ Receives messages as UDP packets. """
    def init(ip_address, port)
        @socket = UDPSocket.new
        @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
        @socket.bind(ip_address, port)
    end

    def recv
        @socket.recv
    end
end

class UdpSender
    """ Sends messages as UDP packets. """
    def init(ip_address, port)
        @socket = UDPSocket.new
        @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
        @socket.connect(ip_address, port)
    end

    def send
        @socket.send(message)
    end
end

class TcpReceiver
    """ Receives data as UDP packets. """
    def init(ip_address, port)
        @socket = accept_connection(port)
    end

    def receive_connection(port)
        TCPServer.new(port).accept
    end

    def recv
        @socket.recv
    end
end

class TcpSender
    """ Sends messages as UDP packets. """
    def init(ip_address, port)
        @socket = TCPSocket.new(ip_address, port)
        @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
    end

    def send(data)
        @socket.send(data)
    end
end

class StdioTransport
    """ Reads and writes messages to stdin and stdout respectively. """
    def recv
        $stdin.read
    end

    def send(data)
        $stdout.write(data)
    end
end

class FileTransport
    """ Reads and writes messages to or from a file. """
    def init(path)
        @path = path
    end

    def recv
        File.read(path)
    end

    def send(data)
        File.open(path, "w+") { |f|
            f.write(data)
        }
    end
end

class FolderReader
    """ Reads messages from a folder """
    def init(path)
        @files = Generator.new(Dir.glob(path))
    end

    def recv
        File.read(next_file)
    end

    def next_file
        @files.next
    end
end

class FolderWriter
    """ Writes messages to a folder """
    def init(path)
        @path = path
        @seq  = 0
        initialise_directory()
    end

    def initialise_directory()
        dirname = File.dirname(@path)
        if not File.directory?
            Dir.mkdir(dirname)
        end
    end

    def next_file()
        filename = @path + "." + @seq.to_str
        @seq = @seq + 1
        filename
    end

    def send(data)
        File.open(next_write, "w") { |f|
            f.write(data)
        }
    end
end

##### Encoding implementations #####

class NullEncoder
    """ Dgram encoder assumes each message is seperated. """
    def encode(message)
        message
    end

    def decode(data)
        data, Null
    end
end

class LengthEncoder

    LENGTH_FORMAT = 'I'
    LENGTH_LENGTH = 4

    def encode(message)
        header = LENGTH_FORMAT.pack(message.length)
        header + message
    end

    def read(data)
        length = data.unpack(LENGTH_FORMAT)
        message[4...4 + length], message[length..-1] 
    end
end

class DelimiterEncoder

    def init(delimiter)
        @start     = true
        @delimiter = delimiter
    end

    def encode(message)
        if @start
            @start = false
            message
        else
            delimiter + message
        end
    end

    def read(data)
        if data.index delimiter
            data.split(delimiter, 2)
        else
            Null, data
        end
    end
end

##### Application #####

class MessageProxy

    def init(options)
      @options=option
      build_stream
    end

    def run
      begin
        process
      rescue
        log "Fatal error: run loop terminated."
      end 
    end

    def process
        log "Message proxy - start>"
        while has_data?
            process_message
        end
        log "Message proxy - end>"
    end

    def build_stream
        @end_of_stream = false
    end

    def has_data?
        not @end_of_stream
    end

    def process_message
      read_message
      write_message
    end

    def read_message
        begin
          @data += @input.recv
        rescue EndOfTransport
        end
    end

    def log(message)
      $stderr.puts message
    end
end

if $0 == 'message_proxy.rb'
    MessageProxyApplication.new(ARGV).run()
end
