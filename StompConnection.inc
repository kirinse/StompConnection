<?php
/**
 * StompFrames are messages that are sent and received on a StompConnection.
 *
 * @package Stomp
 * @author Hiram Chirino <hiram@hiramchirino.com>
 * @author Dejan Bosanac <dejan@nighttale.net>
 * @author Joe Hansche <jhansche@myyearbook.com> - ported to PHP5 and sockets extension
 * @author Gavin M. Roy <gmr@myyearbook.com> - Cleaned up readFrame and writeFrame for stricter protocol adherence.
 */

class StompFrame
{
  public $command;
  public $headers = array();
  public $body;

  function __construct ($command = null, $headers = null, $body = null)
  {
    $this->init($command, $headers, $body);
  }

  function init ($command = null, $headers = null, $body = null)
  {
    $this->command = $command;
    if ( $headers != null )
      $this->headers = $headers;
    $this->body = $body;
  }
}

/**
 * Basic text stomp message
 *
 * @package Stomp
 * @author Dejan Bosanac <dejan@nighttale.net>
 * @version $Revision: 23794 $
 */
class StompMessage extends StompFrame
{
  function __construct($body, $headers = null)
  {
    $this->init('SEND', $headers, $body);
  }
}

/**
 * Message that contains a stream of uninterpreted bytes
 * 
 * @package Stomp
 * @author Dejan Bosanac <dejan@nighttale.net>
 */
class BytesMessage extends StompMessage
{
  function __construct ($body, $headers = null)
  {
    $this->init('SEND', $headers, $body);
    if ( $this->headers == null )
    {
      $this->headers = array();
    }
    $this->headers['content-length'] = count($body);
  }
}

/**
 * Message that contains a set of name-value pairs
 *
 * @package Stomp
 * @author Dejan Bosanac <dejan@nighttale.net>
 */
class MapMessage extends StompMessage
{
  public $map;

  function __construct ($msg, $headers = null)
  {
    if ( $msg instanceOf StompFrame )
    {
      $this->init($msg->command, $msg->headers, $msg->body);
      $this->map = json_decode($msg->body);
    } else
    {
      $this->init('SEND', $headers, $msg);
      if ( $this->headers == null )
      {
        $this->headers = array();
      }
      $this->headers['amq-msg-type'] = 'MapMessage';
      $this->body = json_encode($msg);
    }
  }
}

/**
 * A Stomp Connection
 *
 * @package Stomp
 * @author Hiram Chirino <hiram@hiramchirino.com>
 * @author Dejan Bosanac <dejan@nighttale.net>
 * @author Joe Hansche <jhansche@myyearbook.com> - Sockets Extension 
 * @version $Revision: 23794 $
 */
class StompConnection
{
  const DEFAULT_PORT = 61613;
  
  protected $socket = null;
  protected $hosts = array();
  protected $params = array();
  protected $subscriptions = array();
  protected $currentHost = -1;
  protected $attempts = 10;
  protected $username = 'guest';
  protected $password = 'guest';

  protected static $usageStats = array( 'calls' => array( ) );

  function __construct($brokerUri)
  {
    $uri = parse_url($brokerUri);

    // Failover takes a format of:  failover:(tcp://host1:port1,tcp://host2:port2)?params=vals
    if ( $uri['scheme'] === 'failover' )
    {
      $urls = explode(',', trim($uri['path'], '()'));
      foreach( $urls as $url )
      {
        $tempuri = parse_url($url);
        if ( ! isset( $tempuri['port'] ) ) 
        {
          $tempuri['port'] = self::DEFAULT_PORT;
        }
        $this->hosts[] = array( $tempuri['host'], $tempuri['port']);
      }
      unset($uri['path'], $uri['scheme']);
    }
    // Non-failover format is:  tcp://host:port?params=vals
    else
    {
      if ( ! isset($uri['port']) )
      {
        $uri['port'] = self::DEFAULT_PORT;
      }
      $this->hosts[] = array( $uri['host'], $uri['port'] );
      unset($uri['host'], $uri['scheme']);
    }
    
    if ( isset($uri['user']) )
    {
      $this->username = $uri['user'];
    }
    if ( isset($uri['pass']) )
    {
      $this->password = $uri['pass'];
    }
    
    // Parse the query string as parameters
    if ( isset($uri['query']) )
    {
      parse_str( $uri['query'], $this->params );
    }
    
    if ( ! is_numeric( $this->params['connectionTimeout'] ) )
    {
      // 1 second connection timeout
      $this->params['connectionTimeout'] = 1000;
    }
    
    if ( ! is_numeric( $this->params['soTimeout'] ) )
    {
      // No socket timeout (wait forever)
      $this->params['soTimeout'] = 0;
    }
    
    if ( ! is_numeric( $this->params['socketBufferSize']) )
    {
      // 64KB
      $this->params['socketBufferSize'] = 65536;
    }
    
    $this->makeConnection();
    $this->connect( $this->username, $this->password );
  }

  /**
   * Builds the socket and connection to the server
   *
   * @return boolean
   */
  function makeConnection()
  {
    $startTime = microtime( true );
    if ( count($this->hosts) == 0 )
    {
      self::$usageStats['calls'][] = array( 'command' => 'makeConnection', 'duration' => microtime( true ) - $startTime );
      trigger_error('No broker defined', E_USER_ERROR);
      return false;
    }
    
    $i = $this->currentHost;
    $attempt = 0;
    $connected = false;
    $numHosts = count($this->hosts);
    
    while ( false === $connected && ++$attempt <= $this->attempts )
    {
      if ( $numHosts > 1 && isset( $this->params['randomize'] ) && (bool)($this->params['randomize']) === true )
      {
        $i = rand(0, $numHosts - 1);
      } else
      {
        $i = ($i + 1) % $numHosts;
      }
      
      $broker = $this->hosts[$i];
      
      list( $host, $port ) = $broker;
      
      if ( ! is_numeric( $port ) )
      {
        $port = self::DEFAULT_PORT;
      }
      
      if ( $this->socket !== NULL )
      {
        socket_close($this->socket);
        $this->socket = NULL;
      }
      
      $this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
      
      if ( $this->params['socketBufferSize'] > 0 )
      {
        socket_set_option($this->socket, SOL_SOCKET, SO_SNDBUF, $this->params['socketBufferSize']);
        socket_set_option($this->socket, SOL_SOCKET, SO_RCVBUF, $this->params['socketBufferSize']);
      }
      
      if ( $this->params['soTimeout'] > 0 )
      {
        stream_set_timeout($this->socket, (int)($this->params['soTimeout'] / 1000), (int)($this->params['soTimeout'] % 1000) * 1000 );
        socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, $this->params['soTimeout']);
        socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, $this->params['soTimeout']);
      }

      if ( $this->params['connectionTimeout'] > 0 )
      {
        // Use socket_select() on a non-blocking socket to see if it connected successfully
        socket_set_nonblock( $this->socket );
        socket_connect( $this->socket, $host, $port);
        socket_set_block( $this->socket );
        // socket_select() takes a timeout, so use that for the connection timeout
        $tempSelect = socket_select( $r = array($this->socket), $w = array($this->socket), $f = array($this->socket), (int)($this->params['connectionTimeout'] / 1000), (int)($this->params['connectionTimeout'] % 1000) * 1000 );
        
        switch ( $tempSelect )
        {
          case 2:
            throw new Exception('Stomp Connection refused.  Is the service running?');
            break;
          case 0:
            throw new Exception('Stomp Connection timed out.  Possibly network problems?');
            break;
          case 1:
            $result = true;
            break;
        }
      }
      else
      {
        // For no timeout, just use socket_connect()
        $result = socket_connect( $this->socket, $host, $port);
      }
      
       
      if ( false === $this->socket || $result !== true )
      {
        trigger_error('Could not connect to '.$host.':'.$port.' (Try #'.$attempt.'/'.$this->attempts.')', E_USER_WARNING);
      } else
      {
        $connected = true;
        $this->currentHost = $i;
        break;
      }
    
    }
    
    if ( $connected === false )
    {
      socket_shutdown( $this->socket, 2 );
      socket_close( $this->socket );
      $this->socket = NULL;
      self::$usageStats['calls'][] = array( 'command' => 'makeConnection', 'duration' => microtime( true ) - $startTime );
      trigger_error('Could not connect to a broker', E_USER_ERROR);
      return false;
    }
    
    self::$usageStats['calls'][] = array( 'command' => 'makeConnection', 'duration' => microtime( true ) - $startTime );
    return $connected;
  }

  /**
   * Connect to the amq server
   *
   * @param string $username
   * @param string $password
   * @return StompFrame
   */
  protected function connect ($username = '', $password = '')
  {
    $startTime = microtime( true );
    if ( $username !== '' )
      $this->username = $username;
    
    if ( $password !== '' )
      $this->password = $password;
    
    $this->writeFrame(new StompFrame('CONNECT', array('login' => $this->username, 'passcode' => $this->password)));
    $result = $this->readFrame();

    self::$usageStats['calls'][] = array( 'command' => 'connect', 'duration' => microtime( true ) - $startTime );
    return $result;
  }

  /**
   * Send a message to a queue
   *
   * @param string $destination
   * @param mixed $msg String body, or StompFrame object
   * @param array $properties
   */
  function send ($destination, $msg, $properties = null)
  {
    $startTime = microtime( true );
    if ( $msg instanceOf StompFrame )
    {
      $msg->headers['destination'] = $destination;
      return $this->writeFrame($msg);
    } else
    {
      $headers = array();
      if ( isset($properties) )
      {
        foreach ( $properties as $name => $value )
        {
          $headers[$name] = $value;
        }
      }
      $headers['destination'] = $destination;
      return $this->writeFrame(new StompFrame('SEND', $headers, $msg));
    }
    self::$usageStats['calls'][] = array( 'command' => 'send', 'duration' => microtime( true ) - $startTime );
  }

  /**
   * Get the usage stats across stomp connections
   *
   * @return array $usageStats
   */
  static function getUsageStats( )
  {
    return self::$usageStats;
  }

  /**
   * Register a subscription to a queue
   *
   * @param string $destination Queue name
   * @param array $properties
   */
  function subscribe ($destination, $properties = null)
  {
    $headers = array('ack' => 'client');
    if ( isset($properties) )
    {
      foreach ( $properties as $name => $value )
      {
        $headers[$name] = $value;
      }
    }
    $headers['destination'] = $destination;
    $this->writeFrame(new StompFrame('SUBSCRIBE', $headers));
    $this->subscriptions[$destination] = $properties;
  }

  /**
   * Unsubscribe from a queue destination
   *
   * @param string $destination Queue name
   * @param array $properties
   */
  function unsubscribe ($destination, $properties = null)
  {
    $headers = array();
    if ( isset($properties) )
    {
      foreach ( $properties as $name => $value )
      {
        $headers[$name] = $value;
      }
    }
    $headers['destination'] = $destination;
    $this->writeFrame(new StompFrame('UNSUBSCRIBE', $headers));
    unset($this->subscriptions[$destination]);
  }

  /**
   * Begin a Stomp/AMQ transaction
   *
   * @param string $transactionId
   */
  function begin ($transactionId = null)
  {
    $headers = array();
    if ( isset($transactionId) )
    {
      $headers['transaction'] = $transactionId;
    }
    $this->writeFrame(new StompFrame('BEGIN', $headers));
  }

  /**
   * Commit a pending AMQ transaction
   *
   * @param string $transactionId
   */
  function commit ($transactionId = null)
  {
    $headers = array();
    if ( isset($transactionId) )
    {
      $headers['transaction'] = $transactionId;
    }
    $this->writeFrame(new StompFrame('COMMIT', $headers));
  }

  /**
   * Abort a pending AMQ transaction
   *
   * @param string $transactionId
   */
  function abort ($transactionId = null)
  {
    $headers = array();
    if ( isset($transactionId) )
    {
      $headers['transaction'] = $transactionId;
    }
    $this->writeFrame(new StompFrame('ABORT', $headers));
  }

  /**
   * Acknowledge a pending AMQ message
   *
   * @param string $message amq "message-id" header being acknowledged
   * @param string $transactionId
   */
  function ack ($message, $transactionId = null)
  {
    if ( $message instanceOf StompFrame )
    {
      $this->writeFrame(new StompFrame('ACK', $message->headers));
    } else
    {
      $headers = array();
      if ( isset($transactionId) )
      {
        $headers['transaction'] = $transactionId;
      }
      $headers['message-id'] = $message;
      $this->writeFrame(new StompFrame('ACK', $headers));
    }
  }

  /**
   * Disconnect from AMQ server
   */
  function disconnect ()
  {
    if ( $this->socket )
    {
      $this->writeFrame(new StompFrame('DISCONNECT'));
    }
    socket_shutdown($this->socket, 1);
    usleep(500);
    socket_shutdown($this->socket, 2);
    socket_close($this->socket);
    $this->socket = NULL;
  }

  /**
   * Write a frame
   *
   * @param StompFrame $stompFrame
   */
  protected function writeFrame ($stompFrame)
  {
    $data = $stompFrame->command . "\n";
    if ( isset($stompFrame->headers) )
    {
      foreach ( $stompFrame->headers as $name => $value )
      {
        $data .= $name . ': ' . $value . "\n";
      }
    }
    $data .= "\n";
    if ( isset($stompFrame->body) )
    {
      $data .= $stompFrame->body;
    }
    
    // End the Frame
    $data .= "\0";
    
    $r = socket_write($this->socket, $data);
    
    if ( $r === false || $r == 0 )
    {
      throw new Exception('Could not send Stomp Frame to the broker.');
    }
    
    return ( $r == strlen( $data ) );
  }

  /**
   * Read a StompFrame from the queue
   *
   * @return StompFrame
   */
  function readFrame ()
  {
    $start = microtime( true );

    $data = '';
    $byte = 0x00;
  
    while ( 1 )
    {
      $result = socket_recv($this->socket, $byte, 1, 0);
      if ( $result === false )
      {
        throw Exception('Stomp Disconnected');        
      }  

      // Null byte == end of frame
      if ( ord($byte) == 0 )
      {
        break;
      }
      
      $data .= $byte;
    }
    
    list ($header, $body) = explode("\n\n", $data, 2);
    $header = explode("\n", $header);
    $headers = array();
    
    $command = NULL;
    foreach ( $header as $v )
    {
      if ( isset($command) )
      {
        list ($name, $value) = explode(':', $v, 2);
        $headers[$name] = trim($value);
      } else
      {
        $command = $v;
      }
    }
    
    $frame = new StompFrame($command, $headers, trim($body));
    
    if ( $frame->command == 'ERROR' )
    {
      throw new Exception("Stomp returned Error Frame: "  . $body);
    }
    
    if ( isset($frame->headers['amq-msg-type']) && $frame->headers['amq-msg-type'] == 'MapMessage' )
    {
      return new MapMessage($frame);
    } else {
      return $frame;
    }
  }

}
