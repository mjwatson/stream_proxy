#!/usr/bin/ruby

##### Dependencies ####

require 'socket'
require 'uri'

require 'rubygems'
require 'ffi-rzmq'
require 'json'
require 'xmlsimple'

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

class Sender
    def send(state, message)
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

    def send(state, message)
        @socket.write(message)
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

    def send(state, data)
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

    def send(state, data)
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

    def send(state, data)
        File.open(@path, "a+") { |f|
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

    def send(state, data)
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

class Zmq

  def self.build(pos, options)
    mode, address = options.split(':', 2)
    Zmq.new mode, address
  end

  def initialize(mode, address)
    @address  = address
    convert_mode(mode)
    start_socket
  end

  def start_socket
    @ctx = ZMQ::Context.new
    @s = @ctx.socket @zmq_mode # eg ZMQ::REP
    @s.setsockopt(ZMQ::SUBSCRIBE,'')
    if is_server?
      @s.bind(@address)
    else
      @s.connect(@address)
    end
  end

  def convert_mode(mode)
    table = { 'REQ'    => [ZMQ::REQ, :client, :sync],
              'REP'    => [ZMQ::REP, :server, :sync],
              #'DEALER' => [ZMQ::DEALER,],
              #'ROUTER' => [ZMQ::ROUTER,],
              'PUB'    => [ZMQ::PUB, :server, :async ] ,
              'SUB'    => [ZMQ::SUB, :client, :async ],
              'PUSH'   => [ZMQ::PUSH, :client, :async] ,
              'PULL'   => [ZMQ::PULL, :server, :async],
              #'PAIR'   => [ZMQ::PAIR,],
     }

    @zmq_mode, @type, @sync = table[mode]
  end

  def is_server?
      @type == :server
  end

  def response_required?
      @sync == :sync
  end

  def recv
      msg = ""
      @s.recv_string msg
      if response_required?
        @s.send_string "<Response>", 0
      end
      msg
  end

  def send(state, message)
    @s.send_string(message, 0)
    if response_required?
      stub = ""
      @s.recv_string stub
    end
    [message, nil]
  end

end

##### Encoding implementations #####

class Encoder
  """ Wrap encoder classes to meet sender interface """

  def self.encode(encoder)
    Encoder.new(:encode, encoder)
  end

  def self.decode(encoder)
    Encoder.new(:decode, encoder)
  end

  def initialize(encode_decode, encoder)
    @encode_decode = encode_decode
    @encoder       = encoder
  end

  def build(pos, options)
    build_encoder(pos, options)
    self
  end

  def build_encoder(pos, options)
    if @encoder.respond_to? :build
      @encoder = @encoder.build(pos, options)
    else
      @encoder = @encoder.new
    end
  end

  def send(state, message)
    if @encode_decode == :encode
      [@encoder.encode(message), nil]
    else
      @encoder.decode(state, message)
    end
  end
end

class NullEncoder
    """ Dgram encoder assumes each message is seperated. """
    def send(message)
        [message, nil]
    end
end

class LengthEncoder

    LENGTH_FORMAT = 'I'
    LENGTH_LENGTH = 4

    def encode(message)
        header = [ message.length ].pack(LENGTH_FORMAT)
        header + message
    end

    def decode(state, message)
        length = message.unpack(LENGTH_FORMAT)[0]
        if length
          [message[LENGTH_LENGTH...LENGTH_LENGTH + length], message[LENGTH_LENGTH + length..-1]]
        else
          [nil, nil]
        end
    end
end

class DelimiterBuilder
    def self.build(pos, options)
      DelimiterEncoder.new(options)
    end
end

class DelimiterEncoder
    def initialize(delimiter)
        @start     = true
        @delimiter = delimiter
    end

    def encode(message)
        if @start
            @start = false
            message
        else
            @delimiter + message
        end
    end

    def decode(state, data)
        if data.index @delimiter 
          data.split(@delimiter, 2)
        elsif state == :end
          [data, nil]
        else
            [nil, data]
        end
    end
end

class LinesEncoder < DelimiterEncoder
  def initialize()
    super "\n"
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

    def send(state, data)
        @count += 1
        p "#{@name}: #{state} #{@count} -> #{data}"
        [data, nil]
    end
end

class UrlEncoder
    def encode(message)
      URI.encode(message)
    end

    def decode(state, message)
      [URI.decode(message), nil]
    end
end
    
class NewLineEncoder

    ENCODE = {
      '\\' => '\\\\',
      "\n" => '\n',
      "\r" => '\r'
    }

    DECODE = Hash[ENCODE.map { |k,v| [v,k] }] 

    def encode(message)
      message.gsub(/[\\\n\r]/) { |match| ENCODE[match] }
    end

    def decode(state, message)
      [ message.gsub(/\\./) { |match| DECODE[match] } , nil]
    end
end

class Strip
  def encode(s)
    s.strip
  end
end

class Skip
  def self.build(pos, options)
    Skip.new(options.to_i)
  end

  def initialize(i)
    p i
    @i = i
  end

  def send(state, message)
    if 0 <= @i
     [message[@i..-1], nil]
    else
     [message[0..@i], nil]
    end
  end
end

class JsonConverter
    def to_hash(s)
      JSON.parse(s)
    end

    def from_hash(h)
      h.to_json
    end
end

class XmlConverter
  def to_hash(s)
    XmlSimple.xml_in(s, { 'KeepRoot' => true })
  end

  def from_hash(h)
    XmlSimple.xml_out(h, { 'KeepRoot' => true })
  end
end

class Converter
  def self.build(input, out)
    Converter.new(input.new, out.new)
  end

  def initialize(input, out)
    @input = input
    @out = out
  end

  def build(pos, options)
    self
  end

  def send(state, data)
    [ @out.from_hash(@input.to_hash data), nil ]
  end
end

##### Application #####

class MessageProxy

    def initialize(stream)
        @state  = :start
        @stream = stream
        @cache  = Array.new
    end

    def run
      begin
        @state = :active
        process
      rescue
        log "Fatal error: run loop terminated."
      end 
    end

    def process
        log "Message proxy - start>"
        while active?
            process_message
        end
        log "\nMessage proxy - end>"
    end

    def active?
        @state != :end 
    end

    def process_message
      input = read_message
      write_message(input)
    end

    def read_message
        begin
          input = @stream[0].recv
          if not input
            p "Received nil data..."
          end
          input
        rescue EndOfTransport
          @state = :end
          nil
        end
    end

    def write_message(data)
        do_write_message(data, 1)
    end

    def do_write_message(data, n)

        if not @stream[n]
          return
        end

        if data and @cache[n]
          data = @cache[n] + data
        elsif @cache[n]
          data = @cache[n]
        end

        while (@state == :end) || (data and not data.empty?)

            if data
              message, remaining_data = @stream[n].send(@state, data)
            else
              message, remaining_data = nil, nil
            end

            if message || @state == :end
              do_write_message(message, n + 1)
            end

            processed = data == remaining_data 
            data = remaining_data
            if processed
              break
            end
        end

        @cache[n] = data
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
          'tcp'     => Tcp,
          'udp'     => Udp,
          '-'       => StdioTransport,
          'std'     => StdioTransport,
          'log'     => Logger,
          'file'    => FileTransport,
          'folder'  => Folder,
          'zmq'     => Zmq,
          'null'    => NullEncoder,
          'skip'    => Skip,
          'strip'   => Encoder.encode(Strip),
          '+length' => Encoder.encode(LengthEncoder),
          '-length' => Encoder.decode(LengthEncoder),
          '+delim'  => Encoder.encode(DelimiterBuilder),
          '-delim'  => Encoder.decode(DelimiterBuilder),
          '+lines'  => Encoder.encode(LinesEncoder),
          '-lines'  => Encoder.decode(LinesEncoder),
          '+url'    => Encoder.encode(UrlEncoder),
          '-url'    => Encoder.decode(UrlEncoder),
          '+n'      => Encoder.encode(NewLineEncoder),
          '-n'      => Encoder.decode(NewLineEncoder),
          'xml-json' => Converter.build(XmlConverter,JsonConverter),
          'json-xml' => Converter.build(JsonConverter,XmlConverter),
        }

        name, options = element_spec.split(':', 2)
        element_types[name].build(position, options)
    end
end

if $0 == 'message_proxy.rb'
    MessageProxyApplication.new(ARGV).run()
end
