'''
A partial rewrite of the watchtower package with specialized functionality
'''
import queue, logging, warnings, time, random, collections, json, threading

import boto3, distributed, dask
from botocore.exceptions import ClientError

################################################################################

class CloudWatchWarning(UserWarning):
    pass

################################################################################

def ip():
    try:
        return dask.config.get('cloud.ip')
    except KeyError:
        return distributed.utils.get_ip()

################################################################################

class AwsSession:
    '''Picklable wrapper for a boto3.Session object'''
    def __init__(self, boto=None):
        if boto is None:
            self.boto = boto3.Session()
        elif isinstance(boto, boto3.Session):
            self.boto = boto
        elif hasattr(boto, 'boto'):
            self.boto = boto.boto
        else:
            self.boto = boto3.Session(**boto)

    def __getstate__(self):
        c = self.boto.get_credentials()
        return {
            'aws_access_key_id': c.access_key,
            'aws_secret_access_key': c.secret_key,
            'aws_session_token': c.token,
            'region_name': self.boto.region_name,
            #'profile_name': {'default': None, None: None}.get(self.boto.profile_name, self.boto.profile_name)
        }

    def __setstate__(self, state):
        self.__init__(state)

################################################################################

def retry_log(function, *args, **kwargs):
    '''Retry an AWS logging operation, forever, until it succeeds'''
    t = 1
    while True:
        try:
            return kwargs.get('sequenceToken'), function(*args, **kwargs)
        except ClientError as e:
            err = e.response.get('Error', {}).get('Code')
            if err in ("DataAlreadyAcceptedException", "InvalidSequenceTokenException"):
                kwargs['sequenceToken'] = e.response['Error']['Message'].rsplit(' ', 1)[-1]
            elif err == 'ThrottlingException':
                warnings.warn('Retrying AWS CloudWatch log operation: {}\n'.format(e), CloudWatchWarning)
                time.sleep(t)
                t *= random.random() + int(t < 500)
            else:
                raise

################################################################################

class CloudWatchHandler(logging.Handler):
    '''
    Picklable wrapper for watchtower.CloudWatchLogHandler
    It was necessary to rewrite this class completely because the watchtower
    one uses daemon threads, which didn't work, didn't have our desired retrying
    functionality in the case of ThrottlingException, and we wanted some special
    case functionality to set the default stream to the current public facing IP.
    '''
    END = 1
    FLUSH = 2
    EXTRA_MSG_PAYLOAD_SIZE = 26

    def setup(self, level):
        self.shutting_down = False
        self.client = self.session.boto.client('logs')
        self.stream = self.stream_init or ip() or 'localhost'
        self.queue = queue.Queue()
        self.thread = None
        try:
            retry_log(self.client.create_log_stream, logGroupName=self.group, logStreamName=self.stream)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise
        super().__init__(level=level)

    def put(self, msg):
        '''Thread may appear stopped if the process is forked; restart if so'''
        if self.thread is None or not self.thread.is_alive():
            self.thread = threading.Thread(target=self.batch_sender)
            self.thread.start()
        self.queue.put(msg)

    def __init__(self, group, stream=None, level=0, interval=60, session=None,
                 max_size=1024**2, max_count=10000, default=None):
        self.session = AwsSession(session)
        self.group = str(group)
        self.stream_init = stream
        self.max_count = int(max_count)
        assert self.max_count > 0, 'Maximum count must be positive'
        self.max_size = float(max_size)
        self.interval = float(interval)
        self.default = default
        self.token = None
        self.setup(level)

    def emit(self, record):
        '''Add some keys and dump dict to JSON'''
        if isinstance(record.msg, collections.Mapping):
            msg = dict(scope=record.name, level=record.levelname)
            msg.update(record.msg)
            record.msg = json.dumps(msg, default=self.default)
        record = dict(timestamp=int(record.created * 1000), message=self.format(record))

        if self.shutting_down:
            warnings.warn("Received message after logging system shutdown", CloudWatchWarning)
        self.put(record)

    def _submit_batch(self, batch):
        if not batch:
            return
        sorted_batch = sorted(batch, key=lambda x: x['timestamp'])
        kwargs = dict(logGroupName=self.group, logStreamName=self.stream, logEvents=sorted_batch)
        try:
            token, response = retry_log(self.client.put_log_events, **kwargs)
            if "rejectedLogEventsInfo" in response:
                warnings.warn("Failed to deliver logs: {}".format(response), CloudWatchWarning)
            if token is not None:
                self.token = token
        except Exception as e:
            warnings.warn("Failed to deliver logs: {}".format(e), CloudWatchWarning)

    def flush(self):
        if self.shutting_down:
            return
        self.put(self.FLUSH)

    def __getstate__(self):
        self.flush()
        out = {k: v for k, v in self.__dict__.items() if k not in ('client', 'queue', 'thread', 'lock')}
        return out

    def __setstate__(self, state):
        level = state.pop('level')
        self.__dict__.update(state)
        self.setup(level)

    def batch_sender(self):
        batch, size, deadline = [], 0, None
        while True:
            try:
                msg = self.queue.get(block=True, timeout=max(0, deadline-time.time()) if deadline else None)
            except queue.Empty:
                msg = self.FLUSH

            if isinstance(msg, dict):
                msg_size = len(msg['message']) + self.EXTRA_MSG_PAYLOAD_SIZE
                if msg_size > self.max_size:
                    warnings.warn('Truncated CloudWatch message', CloudWatchWarning)
                    msg['message'] = msg['message'][:self.max_size - msg_size]
                batch.append(msg)
                size += msg_size
                if size > self.max_size or len(batch) > self.max_count:
                    self._submit_batch(batch[:-1])
                    batch, size, deadline = [msg], msg_size, None
                if deadline is None:
                    deadline = time.time() + self.interval
            else:
                assert msg in (self.END, self.FLUSH), 'Unhandled message type {}'.format(type(msg))
                self._submit_batch(batch)
                if msg == self.END:
                    return
                batch, size, deadline = [], 0, None

    def close(self):
        if self.shutting_down:
            return
        self.shutting_down = True
        self.put(self.END)
        self.thread.join()
        super().close()

################################################################################
