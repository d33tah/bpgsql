"""
Barebones PostgreSQL


2001-10-28 Barry Pederson <bp@barryp.org>

    2002-04-06  Changed connect args to be more like the Python DB-API
    2004-03-27  Reworked to follow DB-API 2.0

"""
import select, socket, sys, types
from struct import pack, unpack

#
# Exception hierarchy from DB-API 2.0 spec
#
import exceptions
class Error(exceptions.StandardError):
    pass

class Warning(exceptions.StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass


#
# Custom exceptions raised by this driver
#

class PostgreSQL_Timeout(InterfaceError):
    pass

#
# Constants relating to Large Object support
#
INV_WRITE   = 0x00020000
INV_READ    = 0x00040000

SEEK_SET    = 0
SEEK_CUR    = 1
SEEK_END    = 2

DEBUG = 0



def parseDSN(s):
    """
    Parse a string containg connection info in the form:
       "keyword1=val1 keyword2='val2 with space' keyword3 = val3"
    into a dictionary {'keyword1': 'val1', 'keyword2': 'val2 with space', 'keyword3': 'val3'}

    Returns empty dict if s is empty string or None
    """
    if not s:
        return {}

    result = {}
    state = 1
    buf = ''
    for ch in s.strip():
        if state == 1:        # reading keyword
            if ch in '=':
                keyword = buf.strip()
                buf = ''
                state = 2
            else:
                buf += ch
        elif state == 2:        # have read '='
            if ch == "'":
                state = 3
            elif ch != ' ':
                buf = ch
                state = 4
        elif state == 3:        # reading single-quoted val
            if ch == "'":
                result[keyword] = buf
                buf = ''
                state = 1
            else:
                buf += ch
        elif state == 4:        # reading non-quoted val
            if ch == ' ':
                result[keyword] = buf
                buf = ''
                state = 1
            else:
                buf += ch
    if state == 4:              # was reading non-quoted val when string ran out
        result[keyword] = buf
    return result


class _LargeObject:
    """
    Make a PostgreSQL Large Object look somewhat like
    a Python file.  Should be created from PGClient
    open or create methods.
    """
    def __init__(self, client, fd):
        self.__client = client
        self.__fd = fd

    def __del__(self):
        if self.__client:
            self.close()

    def close(self):
        """
        Close an opened Large Object
        """
        try:
            self.__client._lo_funcall('lo_close', self.__fd)
        finally:
            self.__client = self.__fd = None

    def flush(self):
        pass

    def read(self, len):
        return self.__client._lo_funcall('loread', self.__fd, len)

    def seek(self, offset, whence):
        self.__client._lo_funcall('lo_lseek', self.__fd, offset, whence)

    def tell(self):
        r = self.__client._lo_funcall('lo_tell', self.__fd)
        return unpack('!i', r)[0]

    def write(self, data):
        """
        Write data to lobj, return number of bytes written
        """
        r = self.__client._lo_funcall('lowrite', self.__fd, data)
        return unpack('!i', r)[0]


class PGClient:
    def __init__(self):
        self.__backend_pid = None
        self.__backend_key = None
        self.__socket = None
        self.__input_buffer = ''
        self.__authenticated = 0
        self.__ready = 0
        self.__result = None
        self.__notify_queue = []
        self.__func_result = None
        self.__lo_funcs = {}
        self.__lo_funcnames = {}

    def __del__(self):
        if self.__socket:
            self.__socket.send('X')
            self.__socket.close()
            self.__socket = None


    def __lo_init(self):
        #
        # Make up a dictionary mapping function names beginning with "lo" to function oids
        # (there may be some non-lobject functions in there, but that should be harmless)
        #
        rows = self.execute("SELECT proname, oid FROM pg_proc WHERE proname like 'lo%'")[0]['rows']
        for r in rows:
            oid = int(r[1])
            self.__lo_funcs[r[0]] = oid
            self.__lo_funcnames[oid] = r[0]


    def __read_bytes(self, nBytes):
        #
        # Read the specified number of bytes from the backend
        #
        if DEBUG:
            print '__read_bytes(%d)' % nBytes

        while len(self.__input_buffer) < nBytes:
            d = self.__socket.recv(4096)
            if d:
                self.__input_buffer += d
            else:
                raise OperationalError('Connection to backend closed')
        result, self.__input_buffer = self.__input_buffer[:nBytes], self.__input_buffer[nBytes:]
        return result


    def __read_string(self, terminator='\0'):
        #
        # Read a something-terminated string from the backend
        # (the terminator isn't returned as part of the result)
        #
        result = None
        while 1:
            try:
                result, self.__input_buffer = self.__input_buffer.split(terminator, 1)
                return result
            except:
                # need more data
                d = self.__socket.recv(4096)
                if d:
                    self.__input_buffer += d
                else:
                    raise OperationalError('Connection to backend closed')


    def __read_response(self):
        #
        # Read a single response from the backend
        #  Looks at the next byte, and calls a more specific
        #  method the handle the rest of the response
        #
        #  PostgreSQL responses begin with a single character <c>, this
        #  method looks up a method named _pkt_<c> and calls that
        #  to handle the response
        #
        if DEBUG:
            print '>[%s]' % self.__input_buffer

        pkt_type = self.__read_bytes(1)

        if DEBUG:
            print 'pkt_type:', pkt_type

        method = self.__class__.__dict__.get('_pkt_' + pkt_type, None)
        if method:
            method(self)
        else:
            raise InterfaceError('Unrecognized packet type from server: %s' % pkt_type)


    def __read_row(self, ascii=1):
        #
        # Read an ASCII or Binary Row
        #
        null_byte_count = (self.__field_count + 7) >> 3   # how many bytes we need to hold null bits

        # check if we need to use longs (more than 32 fields)
        if null_byte_count > 4:
            null_bits = 0L
            field_mask = 128L
        else:
            null_bits = 0
            field_mask = 128

        # read bytes holding null bits and setup the field mask
        # to point at the first (leftmost) field
        if null_byte_count:
            for ch in self.__read_bytes(null_byte_count):
                null_bits = (null_bits << 8) | ord(ch)
            field_mask <<= (null_byte_count - 1) * 8

        # read each field into a row
        row = []
        for i in range(self.__field_count):
            if null_bits & field_mask:
                # field has data present, read what was sent
                field_size = unpack('!i', self.__read_bytes(4))[0]
                if ascii:
                    field_size -= 4
                row.append(self.__read_bytes(field_size))
            else:
                # field has no data (is null)
                row.append(None)
            field_mask >>= 1

        self.__result[-1]['rows'].append(row)


    def __send(self, data):
        #
        # Send data to the backend, make sure it's all sent
        #
        if DEBUG:
            print 'Send [%s]' % data

        while data:
            nSent = self.__socket.send(data)
            data = data[nSent:]


    def __wait_response(self, timeout):
        #
        # Wait for something to be in the input buffer, timeout
        # is a floating-point number of seconds, zero means
        # timeout immediately, < 0 means don't timeout (call blocks
        # indefinitely)
        #
        if self.__input_buffer:
            return 1

        if timeout >= 0:
            r, w, e = select.select([self.__socket], [], [], timeout)
        else:
            r, w, e = select.select([self.__socket], [], [])

        if r:
            return 1
        else:
            return 0



    #-----------------------------------
    #  Packet Handling Methods
    #

    def _pkt_A(self):
        #
        # Notification Response
        #
        pid = unpack('!i', self.__read_bytes(4))[0]
        self.__notify_queue.append((self.__read_string(), pid))


    def _pkt_B(self):
        #
        # Binary Row
        #
        print self.__read_row(0)


    def _pkt_C(self):
        #
        # Completed Response
        #
        self.__result[-1]['completed'] = self.__read_string()
        self.__result.append({})


    def _pkt_D(self):
        #
        # ASCII Row
        #
        self.__read_row()


    def _pkt_E(self):
        #
        # Error Response
        #
        if self.__result:
            self.__result[-1]['error'] = self.__read_string()
            self.__result.append({})
        else:
            raise DatabaseError(self.__read_string())


    def _pkt_G(self):
        #
        # CopyIn Response from self.stdin if available, or
        # sys.stdin   Supplies the final terminating line:
        #  '\.' (one backslash followd by a period) if it
        # doesn't appear in the input
        #
        if hasattr(self, 'stdin') and self.stdin:
            stdin = self.stdin
        else:
            stdin = sys.stdin

        lastline = None
        while 1:
            s = stdin.readline()
            if (not s) or (s == '\\.\n'):
                break
            self.__send(s)
            lastline = s
        if lastline and (lastline[-1] != '\n'):
            self.__send('\n')
        self.send('\\.\n')


    def _pkt_H(self):
        #
        # CopyOut Response to self.stdout if available, or
        # sys.stdout    Doesn't write the final terminating line:
        #  '\.'  (one backslash followed by a period)
        #
        if hasattr(self, 'stdout') and self.stdout:
            stdout = self.stdout
        else:
            stdout = sys.stdout

        while 1:
            s = self.__read_string('\n')
            if s == '\\.':
                break
            else:
                stdout.write(s)
                stdout.write('\n')


    def _pkt_I(self):
        #
        # EmptyQuery Response
        #
        print 'Empty Query', self.__read_string()


    def _pkt_K(self):
        #
        # Backend Key data
        #
        self.__backend_pid, self.__backend_key = unpack('!ii', self.__read_bytes(8))
        #print 'Backend Key Data, pid: %d, key: %d' % (self.__backend_pid, self.__backend_key)


    def _pkt_N(self):
        #
        # Notice Response
        #
        print 'Notice:', self.__read_string()


    def _pkt_P(self):
        #
        # Cursor Response
        #
        cursor = self.__read_string()
        #print 'Cursor:', cursor
        self.__result[-1]['rows'] = []


    def _pkt_R(self):
        #
        # Startup Response
        #
        code = unpack('!i', self.__read_bytes(4))[0]
        if code == 0:
            self.__authenticated = 1
            #print 'Authenticated!'
        elif code == 1:
            raise InterfaceError('Kerberos V4 authentication is required by server, but not supported by this client')
        elif code == 2:
            raise InterfaceError('Kerberos V5 authentication is required by server, but not supported by this client')
        elif code == 3:
            self.__send(pack('!i', len(self.__passwd)+5) + self.__passwd + '\0')
        elif code == 4:
            salt = self.__read_bytes(2)
            try:
                import crypt
            except:
                raise InterfaceError('Encrypted authentication is required by server, but Python crypt module not available')
            cpwd = crypt.crypt(self.__passwd, salt)
            self.__send(pack('!i', len(cpwd)+5) + cpwd + '\0')
        elif code == 5:
            import md5

            m = md5.new(self.__passwd + self.__userid).hexdigest()
            m = md5.new(m + self.__read_bytes(4)).hexdigest()
            m = 'md5' + m + '\0'
            self.__send(pack('!i', len(m)+4) + m)
        else:
            raise InterfaceError('Unknown startup response code: R%d (unknown password encryption?)' % code)


    def _pkt_T(self):
        #
        # Row Description
        #
        nFields = unpack('!h', self.__read_bytes(2))[0]
        descr = []
        for i in range(nFields):
            fieldname = self.__read_string()
            oid, type_size, type_modifier = unpack('!ihi', self.__read_bytes(10))
            descr.append((fieldname, oid, type_size, type_modifier))
        self.__field_count = nFields
        self.__result[-1]['description'] = descr


    def _pkt_V(self):
        #
        # Function call response
        #
        self.__func_result = None
        while 1:
            ch = self.__read_bytes(1)
            if ch == '0':
                break
            if ch == 'G':
                result_size = unpack('!i', self.__read_bytes(4))[0]
                self.__func_result = self.__read_bytes(result_size)
            else:
                raise InterfaceError('Unexpected byte: [%s] in Function call reponse' % ch)


    def _pkt_Z(self):
        #
        # Ready for Query
        #
        self.__ready = 1
        #print 'Ready for Query'


    #--------------------------------------
    # Helper func for _LargeObject
    #
    def _lo_funcall(self, name, *args):
        return apply(self.funcall, (self.__lo_funcs[name],) + args)


    #--------------------------------------
    # Public methods
    #

    def close(self):
        self.__del__()


    def connect(self, dsn=None, user='', password='', host=None, database='', port=5432, opt=''):
        """
        Connect to a PostgreSQL server over TCP/IP

        The dsn, if supplied, is in the format used by the PostgreSQL C library, which is one
        or more "keyword=value" pairs separated by spaces.  Values that are single-quoted may
        contain spaces.  Spaces around the '=' chars are ignored.  Recognized keywords are:

          host, port, dbname, user, password, options

        Othewise, the remaining keyword parameters are based somewhat on the Python DB-ABI and
        will fill in anything not specified in the DSN

        """
        #
        # Come up with a reasonable default host for
        # win32 and presumably Unix platforms
        #
        if host == None:
            if sys.platform == 'win32':
                host = '127.0.0.1'
            else:
                host = '/tmp/.s.PGSQL.5432'

        args = parseDSN(dsn)

        if not args.has_key('host'):
            args['host'] = host
        if not args.has_key('port'):
            args['port'] = port
        if not args.has_key('dbname'):
            args['dbname'] = database
        if not args.has_key('user'):
            args['user'] = user
        if not args.has_key('password'):
            args['password'] = password
        if not args.has_key('options'):
            args['options'] = opt

        if args['host'].startswith('/'):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(args['host'])
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((args['host'], int(args['port'])))

        self.__socket = s
        self.__passwd = args['password']
        self.__userid = args['user']

        # Send startup packet specifying protocol version 2.0
        #  (works with PostgreSQL 6.3 or higher?)
        self.__send(pack('!ihh64s32s64s64s64s', 296, 2, 0, args['dbname'], args['user'], args['options'], '', ''))
        while not self.__ready:
            self.__read_response()


    def execute(self, str, args=None, debug=DEBUG):
        if args != None:
            na = []
            for a in args:
                if a == None:
                    a = 'NULL'
                elif type(a) == types.StringType:
                    a = "'" + a.replace('\\', '\\\\').replace("'", "\\'") + "'"
                na.append(a)
            str = str % tuple(na)

        if debug:
            print 'execute', str

        self.__ready = 0
        self.__result = [{}]
        self.__send('Q'+str+'\0')
        while not self.__ready:
            self.__read_response()
        result, self.__result = self.__result[:-1], None
        return result


    def funcall(self, oid, *args):
        """
        Low-level call to PostgreSQL function, you must supply
        the oid of the function, and have the args supplied as
        ints or strings.
        """
        if DEBUG:
            funcname = self.__lo_funcnames.get(oid, str(oid))
            print 'funcall', funcname, args

        self.__ready = 0
        self.__send(pack('!2sii', 'F\0', oid, len(args)))
        for arg in args:
            if type(arg) == types.IntType:
                self.__send(pack('!ii', 4, arg))
            else:
                self.__send(pack('!i', len(arg)))
                self.__send(arg)

        while not self.__ready:
            self.__read_response()
        result, self.__func_result = self.__func_result, None
        return result


    def lo_create(self, mode=INV_READ|INV_WRITE):
        """
        Return the oid of a new Large Object, created with the specified mode
        """
        if not self.__lo_funcs:
            self.__lo_init()
        r = self.funcall(self.__lo_funcs['lo_creat'], mode)
        return unpack('!i', r)[0]


    def lo_open(self, oid, mode=INV_READ|INV_WRITE):
        """
        Open the Large Object with the specified oid, returns
        a file-like object
        """
        if not self.__lo_funcs:
            self.__lo_init()
        r = self.funcall(self.__lo_funcs['lo_open'], oid, mode)
        fd = unpack('!i', r)[0]
        lobj =  _LargeObject(self, fd)
        lobj.seek(0, SEEK_SET)
        return lobj


    def lo_unlink(self, oid):
        """
        Delete the specified Large Object
        """
        if not self.__lo_funcs:
            self.__lo_init()
        self.funcall(self.__funcs['lo_unlink'], oid)


    def wait_for_notify(self, timeout=-1):
        """
        Wait for an async notification from the backend, which comes
        when another client executes the SQL command:

           NOTIFY name

        where 'name' is an arbitrary string. timeout is specified in
        floating- point seconds, -1 means no timeout, 0 means timeout
        immediately if nothing is available.

        In practice though the timeout is a timeout to wait for the
        beginning of a message from the backend. Once a message has
        begun, the client will wait for the entire message to finish no
        matter how long it takes.

        Return value is a tuple: (name, pid) where 'name' string
        specified in the NOTIFY command, and 'pid' is the pid of the
        backend process that processed the command.

        Raises an exception on timeout
        """
        while 1:
            if self.__notify_queue:
                result, self.__notify_queue = self.__notify_queue[0], self.__notify_queue[1:]
                return result
            if self.__wait_response(timeout):
                self.__read_response()
            else:
                raise PostgreSQL_Timeout()


def connect(dsn=None, user='', password='', host=None, database='', port=5432, opt=''):
    pg = PGClient()
    pg.connect(dsn, user, password, host, database, port, opt)
    return pg

# ---- EOF ----
