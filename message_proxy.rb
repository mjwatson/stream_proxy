#!/usr/bin/ruby

##### Dependencies ####

require 'socket'

##### Interfaces #####

class EndOfTransport < IOError
  """ Exception indicating the end of the data stream. """
end

class InvalidData < RuntimeError
  """ Exception indicating data cannot be encoded or decoded """
end

class InvalidOption < RuntimeError
  """ Exeception indicating options are not valid. """
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
    def initialize(ip_address, port)
        @socket = UDPSocket.new
        @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
        @socket.bind(ip_address, port)
    end

    def recv
        @socket.recvfrom(2000)[0]
    end
end

class UdpSender
    """ Sends messages as UDP packets. """
    def initialize(ip_address, port)
        @socket = UDPSocket.new
        @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
        @socket.connect(ip_address, port)
    end

    def send(message)
        p @socket.write(message)
        [ message, nil ]
    end
end

class TcpReceiver
    """ Receives data as UDP packets. """
    def initialize(ip_address, port, keeplistening = true)
        @server        = TCPServer.new(port)
        @keeplistening = keeplistening
    end

    def receive_connection
        @socket = @server.accept
    end

    def recv
        if not @socket
          receive_connection
        end
 
        data = @socket.recv(1024)
        if not data.empty?
            data
        else
            @socket.close
            @socket = nil
            if not @keeplistening
              raise EndOfTransport
            end
        end
    end
end

class TcpSender
    """ Sends messages as UDP packets. """
    def initialize(ip_address, port)
        @socket = TCPSocket.new(ip_address, port)
        @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
    end

    def send(data)
        @socket.write(data)
        [ data, nil ]
    end
end


class AddressBuilder
  
    def self.build(pos, options, in_class, out_class)
        receiver = (pos == 0)
        address, port = parse(options)

        if receiver
          in_class.new(address, port)
        else
          out_class.new(address, port)
        end
    end

    def self.parse(options)
      begin
        m = /(\/\/)?(?<address>[0-9.]+):(?<port>[0-9]+)/.match(options)
        address = m[:address]
        port    = m[:port].to_i
        [address, port]
      rescue
        raise InvalidOption
      end
    end
end

class Tcp
    def self.build(pos, options)
        AddressBuilder.build(pos, options, TcpReceiver, TcpSender)
    end
end


class Udp
    def self.build(pos, options)
        AddressBuilder.build(pos, options, UdpReceiver, UdpSender)
    end
end

class StdioTransport
    """ Reads and writes messages to stdin and stdout respectively. """

    def self.build(pos, options)
        StdioTransport.new
    end

    def recv
        if not $stdin.closed?
            data = $stdin.read
            $stdin.close
            data
        else
            raise EndOfTransport
        end
    end

    def send(data)
        $stdout.write(data)
        [ data, nil ]
    end
end

class FileTransport
    """ Reads and writes messages to or from a file. """

    def self.build(pos, options)
        FileTransport.new(options)
    end

    def initialize(path)
        @path = path
    end

    def recv
        if not @read
          @read = true
          File.read(@path)
        else
          raise EndOfTransport
        end
    end

    def send(data)
        File.open(path, "w+") { |f|
            f.write(data)
        }
    end
end

class Count
    def initialize(count = 0)
      @count = 0
    end

    def next
      result = @count
      @count += 1
      result
    end
end

class FolderReader
    """ Reads messages from a folder """
    def initialize(path)
        @files = Dir.glob(path + "/*")
        @index = Count.new
    end

    def recv
      begin
        File.read(next_file)
      rescue
        raise EndOfTransport  
      end
    end

    def next_file
      @files[@index.next]
    end
end

class FolderWriter
    """ Writes messages to a folder """

    def initialize(path)
        @path = initialise_path(path)
        @seq  = Count.new
        initialise_directory()
    end

    def initialise_path(path)
      if path.length == 0 || path[-1] == '/'
        path + "out"
      else
        path
      end
    end

    def initialise_directory()
      dirname = File.dirname(@path)
      if not File.directory?(dirname)
        Dir.mkdir(dirname)
      end
    end

    def next_file()
      @path + "." + @seq.next.to_s
    end

    def send(data)
      File.open(next_file, "w") { |f|
        f.write(data)
        [data, nil]
      }
    end
end

class Folder
    def self.build(pos, options)
        if pos == 0
          FolderReader.new(options)
        else
          FolderWriter.new(options)
        end
    end
end

##### Encoding implementations #####

class NullEncoder
    """ Dgram encoder assumes each message is seperated. """
    def encode(message)
        message
    end

    def decode(data)
        [data, nil]
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
        [message[4...4 + length], message[length..-1]]
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
            [nil, data]
        end
    end
end

class Logger

    def self.build(position, options)
        Logger.new(options)
    end

    def initialize(name = "LOG")
        @count = 0
        @name  = name
    end

    def send(data)
        @count += 1
        p "#{@name}: #{@count} -> #{data.length}"
        [data, nil]
    end
end
    

##### Application #####

class MessageProxy

    def initialize(stream)
        @state  = :start
        @stream = stream
        @data   = ""
    end

    def run
      #begin
        @state = :active
        process
      #rescue
      #  log "Fatal error: run loop terminated."
      #end 
    end

    def process
        log "Message proxy - start>"
        while active?
            process_message
        end
        log "Message proxy - end>"
    end

    def active?
        @state != :end 
    end

    def process_message
      read_message
      write_message
    end

    def read_message
        begin
          input = @stream[0].recv
          if input
            @data = (@data || "") + input
          else
            p "Received nil data..."
          end
        rescue EndOfTransport
          @state = :end
        end
    end

    def write_message()
        @data = do_write_message(@data, @stream[1..-1])
    end

    def do_write_message(data, targets)
        while data and 0 < data.length and not targets.empty?
            message, data = targets[0].send(data)
            if message
              do_write_message(message, targets[1..-1])
            end
        end
        data
    end

    def log(message)
      $stderr.puts message
    end
end

class MessageProxyApplication

    def initialize(args)
        @message_proxy = build_message_proxy(args)
    end

    def run
        @message_proxy.run
    end

    def build_message_proxy(args)
        MessageProxy.new build_stream(args)
    end

    def build_stream(args)
        args.each_with_index.map do |a, n|
            stream_element(n, a)
        end
    end

    def stream_element(position, element_spec)

        element_types = {
          'tcp'    => Tcp,
          'udp'    => Udp,
          '-'      => StdioTransport,
          'std'    => StdioTransport,
          'log'    => Logger,
          'file'   => FileTransport,
          'folder' => Folder
        }

        name, options = element_spec.split(':', 2)
        element_types[name].build(position, options)
    end
end

if $0 == 'message_proxy.rb'
    MessageProxyApplication.new(ARGV).run()
end
