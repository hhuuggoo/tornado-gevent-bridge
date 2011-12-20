import logging
import zmq
import zmq.eventloop as eventloop
import zmq.eventloop.ioloop as ioloop
import zmq.eventloop.zmqstream as zmqstream
import weakref
import tornado.web
import cPickle as pickle


class SafeZMQMixin(object):
    """
    zeromq doesn't catch excpetions the way tornado does,
    a problem in a callback can take down the entire event loop
    """
    def on_recv(self, recv_function):
        def wrapped(*args, **kwargs):
            try:
                recv_function(*args, **kwargs)
            except Exception as e:
                logging.exception(e)
        self.stream.on_recv(wrapped)

        

class TornadoZMQRequester(SafeZMQMixin):
    def __init__(self, zmq_conn_string, ctx=None):
        if ctx is None: ctx = zmq.Context()
        self.ctx = ctx
        self.zmq_conn_string = zmq_conn_string
        self.s = self.ctx.socket(zmq.REQ)
        self.s.connect(self.zmq_conn_string)
        self.stream = zmqstream.ZMQStream(
            self.s, ioloop.IOLoop.instance()
            )
        #busy flag, tracks if we're in the middle of a req rep
        #ping pong
        self.busy = False
        
    def close(self):
        self.stream.close()
        self.s.close()

    def send_multipart(self, multipart_msg):
        self.stream.send_multipart(multipart_msg)

        
        

class TornadoZMQConnectionPool(object):
    def __init__(self, zmq_conn_string, connection_class,
                 min_connections=1,
                 max_connections=100, ctx=None):
        self.connection_class=connection_class
        self.min_connections=min_connections
        self.max_connections=max_connections
        if ctx is None: ctx = zmq.Context()
        self.ctx = ctx
        self.zmq_conn_string = zmq_conn_string
        self.active_connections = set()
        self.inactive_connections = []

    def _new_connection(self):
        return self.connection_class(self.zmq_conn_string,
                                     ctx=self.ctx)
            
    def get(self):
        if len(self.inactive_connections) == 0:
            if self.max_connections is not None and \
               len(self.active_connections) >= self.max_connections:
                return None
            else:
                conn =  self._new_connection()
        else:
            conn = self.inactive_connections.pop(0)
        self.active_connections.add(conn)
        return conn

    def put(self, connection):
        self.active_connections.remove(connection)
        self.inactive_connections.append(connection)



class ZMQRequestHandler(tornado.web.RequestHandler):
    pool = None
    def serialize(self, obj):
        return pickle.dumps(obj)
    
    def deserialize(self, obj):
        return pickle.loads(obj)
        
    @tornado.web.asynchronous
    def get(self):
        self._handle_request_with_zmq(self.create_get_message,
                                      self.get_message_handler)
    
    @tornado.web.asynchronous
    def post(self):
        self._handle_request_with_zmq(self.create_post_message,
                                      self.post_message_handler)

    """
    override the following
    """
    def _handle_request_with_zmq(self, get_func, handle_func):
        multipart_msg = self.create_get_message()

        connection = self.pool.get()
        def handle_function_and_return_to_pool(multipart_msg):
            handle_func(multipart_msg)
            self.pool.put(connection)
        connection.on_recv(handle_function_and_return_to_pool)
        
        connection.send_multipart(multipart_msg)

    def big_message(self, multipart_msg):
        big_message = reduce(lambda x,y: x + y, multipart_msg)
        return big_message
    
    def create_get_message(self):
        """
        should return a multipart_msg
        """
        pass
    
    def get_message_handler(self, multipart_msg):
        """
        should finish the request, with someting like
        self.render, or self.write and then self.finish, etc..
        """
        pass
    
    def create_post_message(self):
        pass

    def post_message_handler(self, multipart_msg):
        pass
    
