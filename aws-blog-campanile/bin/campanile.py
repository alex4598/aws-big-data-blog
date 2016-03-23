import os
import re
import math
from sys import stderr
from time import sleep
from random import randint

# -----------------------------------------------------------------------------
# Global
# -----------------------------------------------------------------------------
ETAG_PARTCOUNT = re.compile('-(\d+)$')
NULL = '\N'
MAX_PARTS = 10000

CONSTANTS = {
    'KB':  1 * (1024 ** 1),
    'MB':  1 * (1024 ** 2),
    'GB':  1 * (1024 ** 3),
    'KiB': 1 * (1000 ** 1),
    'MiB': 1 * (1000 ** 2),
    'GiB': 1 * (1000 ** 3)
}

SPECIAL_PART_SIZES = [
        7 * CONSTANTS['MB'],
        67108536,
        134217696,
        268434144,
        268435392,
        536869740
]

MAX_SINGLE_UPLOAD_SIZE = 5 * CONSTANTS['GB']
MINIMUM_PART_SIZE = 5 * CONSTANTS['MB']
DEFAULT_PART_SIZE = 8 * CONSTANTS['MB']

CFGFILES = [
    "/etc/campanile.cfg"
]


# -----------------------------------------------------------------------------
# Progress Class - Mapper requires status message every xxx seconds or will 
#                  timeout.      
# -----------------------------------------------------------------------------
class FileProgress:
    def __init__(self, name, verbose=0):
        self.fp = 0
        self.total = None
        self.verbose = verbose
        self.name = name

    def progress(self, fp, total):
        self.total = total
        self.fp = fp
        if self.verbose == 1:
            stderr.write("reporter:status:%s:%s out of %s complete\n" % \
                    (self.name, fp, total))

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
def random_sleep(maxsleep=5):
    sleep(randint(0,maxsleep))

def cli_chunksize(size, partcount):
    computedPartSize = CONSTANTS['MB'];
    minPartSize = objectSize;
    maxPartSize = MAX_SINGLE_UPLOAD_SIZE;

    if (partcount > 1):
        minPartSize = Math.ceil(float(objectSize)/partcount);
        maxPartSize = Math.floor(float(objectSize)/(partcount - 1))

    # Detect using standard MB and the power of 2
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = CONSTANTS['MB']
        while (computedPartSize < minPartSize):
            computedPartSize *= 2

    # Detect using MiB notation and the power of 2
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 1 * CONSTANTS['MiB']
        while (computedPartSize < minPartSize):
            computedPartSize *= 2

    # Detect other special cases like s3n and Aspera
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 1 * CONSTANTS['MiB']
        for value in SPECIAL_PART_SIZES:
            if computedPartSize >= value:
                break
            else:
                computedPartSize = value

    # Detect if using 100MB increments
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 100 * CONSTANTS['MB']
        while computedPartSize < minPartSize:
            computedPartSize += 100 * CONSTANTS['MB']

    # Detect if using 25MB increments up to 1GB
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 25 * CONSTANTS['MB']
        while computedPartSize < minPartSize or computedPartSize < 1 * MirrorConstants.GB:
            computedPartSize += 25 * CONSTANTS['MB']

    # Detect if using 10MB increments up to 1GB
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 10 * CONSTANTS['MB']
        while computedPartSize < minPartSize or computedPartSize < 1 * MirrorConstants.GB:
            computedPartSize += 10 * CONSTANTS['MB']

    # Detect if using 5MB increments up to 1GB
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 5 * CONSTANTS['MB']
        while computedPartSize < minPartSize or computedPartSize < 1 * MirrorConstants.GB:
            computedPartSize += 5 * CONSTANTS['MB']

    # Detect if using 1MB increments up to 100MB
    if computedPartSize < minPartSize or computedPartSize > maxPartSize:
        computedPartSize = 1 * CONSTANTS['MB']
        while computedPartSize < minPartSize or computedPartSize < 100 * CONSTANTS['MB']:
            computedPartSize += 1 * CONSTANTS['MB']

    partSize = computedPartSize

    if computedPartSize > maxPartSize:
        partSize = optionsUploadPartSize

    if computedPartSize < MINIMUM_PART_SIZE:
        partSize = MINIMUM_PART_SIZE
    return partSize

def stream_index():
    try:
        if os.environ['mapred_input_format_class'] == \
                'org.apache.hadoop.mapred.lib.NLineInputFormat':
            if os.environ['mapreduce_task_ismap'] == "true":
                return 1
            else:
                return 0
    except:
        return 0

def counter(group, counter, amount):
    stderr.write("reporter:counter:%s,%s,%s\n" % (group, counter, amount))

def status(msg):
    stderr.write("reporter:status:%s\n" % msg)

def partcount(etag):
    match = ETAG_PARTCOUNT.search(etag.replace("\"", ""))
    if match:
        return int(match.group(1))
    else:
        return 0

def random_sleep(maxsleep=5):
    sleep(randint(0,maxsleep))

def config_section():
    return os.path.splitext(os.path.basename(sys.argv[0]))[0]

def cfg_file_locations():
    return list(CFGFILES)
