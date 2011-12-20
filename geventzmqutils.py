import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent import spawn
from gevent_zeromq import zmq
import cPickle as pickle

class GeventZMQServer(object):
    """Gevent, ZMQ rpc server using XREP socket
    
    Parameters
    ----------
    zmq_conn_string : zmq connection string to the outside world,
       we bind to this, other people connect to this

    Optional Parameters
    ----------
    ctx : zmq context
    """
    def __init__(self, zmq_conn_string, ctx=None):
        self.zmq_conn_string = zmq_conn_string
        if ctx is None: ctx = zmq.Context() 
        self.ctx = ctx
        self.socket = self.ctx.socket(zmq.XREP)
        
    def bind(self):
        self.socket.bind(self.zmq_conn_string)

    def serve(self):
        """
        individual worker function, you should override this method
        """
        while True:
            multipart_msg = self.socket.recv_multipart()
            spawn(self._run, multipart_msg)
            
    def _run(self, multipart_msg):
        null_idx = multipart_msg.index('')
        routing_information = multipart_msg[:null_idx]
        payload = multipart_msg[(null_idx + 1):]
        return_value = self.run(payload)
        new_message = routing_information
        new_message.append('')
        new_message.extend(return_value)
        self.socket.send_multipart(new_message)

    def serialize(self, obj):
        return pickle.dumps(obj)
    
    def deserialize(self, obj):
        return pickle.loads(obj)
                            
    def run(self, multipart_msg):
        """
        override this function
        output should be a list,
        since we're setup to handle
        multipart data
        """
        print multipart_msg
        print 'fakewaiting'
        gevent.sleep(3)
        print 'done fakewaiting'
        return multipart_msg
            
if __name__ == "__main__":
    server = GeventZMQServer("tcp://127.0.0.1:5560")
    server.bind()
    server.serve()

