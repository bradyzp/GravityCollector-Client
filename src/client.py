# -*- coding: utf-8 -*-
import logging
from json import JSONDecodeError
from queue import Queue, Empty
from threading import Event, Thread
from typing import Union
from urllib.parse import urljoin

import requests
import serial
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from urllib3 import Retry

from src.helpers import get_extractor, decode_bytearr, read_config

LOG = logging.getLogger(__name__)


class SerialListener:
    def __init__(self, device_path: str, timeout: int = 1, config: dict = None):
        self._device = device_path
        self._queue = Queue()
        self._exiting = Event()
        self._buffer = bytearray()
        self._config = dict(
            baudrate=57600,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
            timeout=timeout
        )
        if config is not None:
            self._config.update(config)

        self._handle = None

    def open(self):
        self._handle = serial.Serial(port=self._device, **self._config)
        LOG.info(f'Opened serial handle on {self._device}')

    def exit(self):
        self._exiting.set()

    @property
    def collector(self):
        return self._queue

    @property
    def exit_sig(self):
        return self._exiting

    def readline(self):
        """Read a line of data (terminated by \n) from the serial handle

        This method drastically reduces CPU usage of the utility (from ~50%
        when reading 10hz gravity data to ~27% on a raspberry pi zero)

        Credit for this function to skoehler (https://github.com/skoehler) from
        https://github.com/pyserial/pyserial/issues/216

        """
        i = self._buffer.find(b"\n")
        if i >= 0:
            line = self._buffer[:i + 1]
            self._buffer = self._buffer[i + 1:]
            return line
        while not self._exiting.is_set():
            i = max(1, min(2048, self._handle.in_waiting))
            data = self._handle.read(i)
            if data == b'':
                return ''
            i = data.find(b"\n")
            if i >= 0:
                line = self._buffer + data[:i + 1]
                self._buffer[0:] = data[i + 1:]
                return line
            else:
                self._buffer.extend(data)
        return b''

    def listen(self):
        LOG.debug('Running Serial Listener Thread')
        if self._handle is None:
            self.open()
        while not self._exiting.is_set():
            line = decode_bytearr(self.readline())
            if line is None or line == '':
                continue
            self._queue.put_nowait(line)


class HTTPSender(Thread):
    """HTTPSender class creates a threaded object to execute http data sending

    Parameters
    ----------
    queue: Queue
    exit_sig: Event
    session: requests.Session
    host: str
    sensor_id: int
    sensor_type: str
    meter_config

    """

    fields = ['gravity', 'long_acc', 'cross_acc', 'beam', 'pressure',
              'e_temperature', 's_temperature', 'latitude', 'longitude',
              'datetime']

    def __init__(self, queue: Queue, exit_sig: Event, session: requests.Session,
                 host: str, sensor_id: int, sensor_type: str, meter_config=None):
        super().__init__(name='HTTPSender')
        self._queue = queue
        self._exiting = exit_sig
        self.session = session
        self._uri = urljoin(host, f'/collect/{sensor_id}')
        self.sensor_type = sensor_type
        self.meter_config = meter_config

    def exit(self):
        self._exiting.set()

    def _send_line(self, line_json):
        try:
            response = self.session.post(self._uri, json=line_json)
            return response.json()
        except (ConnectionError, ConnectionRefusedError):
            LOG.error("Couldn't connect, exhausted max retries")
            return {'Status': 'FAIL', 'Reason': 'Max Retries Exhausted'}
        except JSONDecodeError:
            return {'Status': 'FAIL', 'Reason': 'JSONDecodeError'}

    def run(self):
        extractor = get_extractor(self.sensor_type)
        batch = 10
        batch_queue = []

        while not self._exiting.is_set():
            try:
                line: str = self._queue.get(block=True, timeout=2)
            except Empty:
                LOG.debug("No data recv from queue")
                continue

            data = extractor(line, self.fields)
            if data is None:
                LOG.warning(f"Unable to extract fields from line {line}")
                self._queue.task_done()
                continue

            batch_queue.insert(0, data)
            self._queue.task_done()
            if len(batch_queue) < batch:
                continue

            payload = {'data': batch_queue, 'length': len(batch_queue)}

            try:
                result = self._send_line(payload)
            except ConnectionError:
                LOG.exception('Send failed')
                continue
            if result.get('Status', 'FAIL') == 'OK':
                LOG.debug(f'Sent {len(batch_queue)} lines of data')
                batch_queue.clear()
                LOG.debug(f'Send Status: {result.get("Status")}, '
                          f'Count: {result.get("Count", -1)}')
            else:
                LOG.warning("Send line failed")
                LOG.warning(result)

        LOG.debug("HTTPSender exiting (signal set)")


def get_session(apikey):
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer: {apikey}'})
    retries = Retry(backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def establish_connection(session: requests.Session, host: str, sensor_name: str,
                         sensor_type: str) -> Union[int, None]:
    uri = urljoin(host, f'/sensor/{sensor_type}/{sensor_name}')
    try:
        response = session.get(uri)
        data = response.json()
    except ConnectionError:
        LOG.exception(f'Unable to connect to collector endpoint {uri}')
        return None
    except JSONDecodeError:
        return None

    if response.status_code != 200:
        LOG.error(f'Error connecting to collector endpoint: '
                  f'{response.status_code}, Reason: {response.reason}')
        return None

    if data['Exists']:
        sid = data.get('SensorID', None)
        LOG.debug(f'Received sensor ID {sid} from endpoint')
        return sid
    else:
        try:
            response = session.put(uri, json={})
            data = response.json()
        except ConnectionError:
            return None
        except JSONDecodeError:
            return None
        sid = data.get('SensorID', None)
        LOG.info(f'Registered new sensor, ID {sid}')
        return sid


def run_client(device: str, apikey: str, host: str, sensor_name: str = None,
               sensor_type: str = None, **cfg):
    if 'meterini' in cfg:
        meter_cfg = read_config(cfg['meterini'])
        LOG.debug(f'Read Meter.ini configuration: {meter_cfg}')
    else:
        meter_cfg = {}

    session = get_session(apikey)
    sensor_id = None
    while sensor_id is None:
        LOG.info("Establishing HTTP connection")
        sensor_id = establish_connection(session, host, sensor_name, sensor_type)

    listener = SerialListener(device)
    http_sender = HTTPSender(listener.collector, listener.exit_sig, session,
                             host, sensor_id, sensor_type, meter_cfg)

    try:
        http_sender.start()
        listener.listen()
    except KeyboardInterrupt:
        LOG.info('Keyboard Interrupt captured, cleanly exiting')
        http_sender.exit()
        http_sender.join(timeout=5)
        return 0
    return 1
