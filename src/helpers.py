# -*- coding: utf-8 -*-
import configparser
import logging
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import List, Dict, Union

__all__ = ['decode_bytearr', 'get_extractor', 'read_config']
LOG = logging.getLogger(__name__)

"""Utility functions and helpers for data extraction/formatting"""

_marine_fieldmap = ['header', 'gravity', 'long_acc', 'cross_acc', 'beam',
                    's_temperature', 'pressure', 'e_temperature',
                    'vcc', 've', 'al', 'ax', 'status', 'checksum',
                    'latitude', 'longitude', 'speed', 'course', 'datetime']
_airborne_fieldmap = ['header', 'gravity', 'long_acc', 'cross_acc', 'beam',
                      's_temperature', 'status', 'pressure', 'e_temperature',
                      'latitude', 'longitude']

ILLEGAL_CHARS = list(chain(range(0, 32), [255, 256]))


def convert_marine_time(meter_time):
    """Convert a AT1M Marine datetime string to a unix timestamp"""
    fmt = '%Y%m%d%H%M%S'
    try:
        return datetime.strptime(meter_time, fmt).timestamp()
    except ValueError:
        return datetime.utcnow().timestamp()


def convert_gps_time(gpsweek, gpsweekseconds):
    """Converts a GPS time format (weeks + seconds since 6 Jan 1980) to a UNIX
    timestamp (seconds since 1 Jan 1970) without correcting for UTC leap
    seconds.

    Static values gps_delta and gpsweek_cf are defined by the below functions
    (optimization) gps_delta is the time difference (in seconds) between UNIX
    time and GPS time.

    gps_delta = (dt.datetime(1980, 1, 6) - dt.datetime(1970, 1, 1)).total_seconds()

    gpsweek_cf is the coefficient to convert weeks to seconds
    gpsweek_cf = 7 * 24 * 60 * 60  # 604800

    Parameters
    ----------
    gpsweek : int
        Number of weeks since beginning of GPS time (1980-01-06 00:00:00)

    gpsweekseconds : float
        Number of seconds since the GPS week parameter

    Returns
    -------
    float or :obj:`datetime`
        UNIX timestamp (number of seconds since 1970-01-01 00:00:00) without
        leap-seconds subtracted
    """
    # GPS time begins 1980 Jan 6 00:00, UNIX time begins 1970 Jan 1 00:00
    gps_delta = 315964800.0
    gpsweek_cf = 604800

    gps_ticks = (float(gpsweek) * gpsweek_cf) + float(gpsweekseconds)

    timestamp = gps_delta + gps_ticks

    return timestamp


def decode_bytearr(bytearr, encoding='utf-8'):
    if isinstance(bytearr, str):
        return bytearr
    try:
        raw = bytes([c for c in bytearr if c not in ILLEGAL_CHARS])
        decoded = raw.decode(encoding, errors='ignore').strip('\r\n')
    except AttributeError:
        decoded = None
    return decoded


_field_casts = {
    'header': str,
    'latitude': float,
    'longitude': float,
    'speed': float,
    'course': float,
    'datetime': convert_marine_time
}


def _extract_airborne_fields(data: str, fields: List[str]):
    extracted = {}
    data: List[str] = data.split(',')
    # GPS Week/seconds fields not included in fieldmap

    if not (len(data) == (len(_airborne_fieldmap) + 2)):
        LOG.error(f'Data and field-map lengths do not match. {len(data)} != {len(_airborne_fieldmap)}')
        return None
    try:
        gpssecond = float(data.pop())
        gpsweek = int(data.pop())
        dt = convert_gps_time(gpsweek, gpssecond)
    except (ValueError, TypeError):
        return None

    for i, field in enumerate(_airborne_fieldmap):
        field = field.lower()
        if field in fields:
            try:
                extracted[field] = _field_casts.get(field, int)(data[i])
            except ValueError:
                extracted[field] = data[i]
            except IndexError:
                return None
    extracted['datetime'] = dt
    return extracted


def _extract_marine_fields(data: str, fields: List[str]):
    """Extract fields from raw ASCII data line, and perform type casts"""
    extracted = {}
    data = data.split(',')
    if not len(data) == len(_marine_fieldmap):
        LOG.error(f"Data and field-map lengths do not match.\nData: {data}")
        return None
    for i, field in enumerate(_marine_fieldmap):
        if field.lower() in fields:
            try:
                extracted[field] = _field_casts.get(field.lower(), int)(data[i])
            except ValueError:
                extracted[field] = data[i]
            except IndexError:
                return None
    return extracted


def get_extractor(sensor_type: str):
    if sensor_type.upper() == 'AT1M':
        return _extract_marine_fields
    else:
        return _extract_airborne_fields


sensor_fields = ['g0', 'GravCal', 'LongCal', 'CrossCal', 'LongOffset', 'CrossOffset', 'stempgain',
                 'Temperature', 'stempoffset', 'pressgain', 'presszero', 'beamgain', 'beamzero',
                 'Etempgain', 'Etempzero', 'Meter']
# Cross coupling Fields
cc_fields = ['vcc', 've', 'al', 'ax', 'monitors']

# Platform Fields
platform_fields = ['Cross_Damping', 'Cross_Periode', 'Cross_Lead', 'Cross_Gain', 'Cross_Comp',
                   'Cross_Phcomp', 'Cross_sp', 'Long_Damping', 'Long_Period', 'Long_Lead', 'Long_Gain',
                   'Long_Comp', 'Long_Phcomp', 'Long_sp', 'zerolong', 'zerocross', 'CrossSp', 'LongSp']

valid_fields = set().union(sensor_fields, cc_fields, platform_fields)


def read_config(path: Path) -> Dict[str, Union[str, int, float]]:
    if not path.exists():
        raise FileNotFoundError
    config = configparser.ConfigParser(strict=False)
    try:
        config.read(str(path))
    except configparser.MissingSectionHeaderError:
        return {}

    sensor_fld = dict(config['Sensor'])
    xcoupling_fld = dict(config['crosscouplings'])
    platform_fld = dict(config['Platform'])

    def safe_cast(value):
        try:
            return float(value)
        except ValueError:
            return value

    merged = {**sensor_fld, **xcoupling_fld, **platform_fld}
    return {k.lower(): safe_cast(v) for k, v in merged.items() if k.lower() in map(str.lower, valid_fields)}

