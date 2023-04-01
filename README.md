# CLP Python Logging Library

This is a Python `logging` library meant to supplement [CLP (Compressed Log Processor)][0].
Logs are compressed in a streaming fashion into CLP's Internal Representation (IR) format before written to disk.
More details are described in this [Uber's blog][1].

Logs compressed in IR format can be viewed in a [log viewer][2] or programmatically analyzed using
APIs provided here. They can also be decompressed back into plain-text log files using [CLP][0] (in a future release).

To achieve the best compression ratio, CLP should be used to compress large
batches of logs, one batch at a time. However, individual log
files are generally small and are generated across a long period of time.

This logging library helps solve this problem by logging directly in CLP's
Internal Representation (IR). A log created with a CLP logging handler is first
parsed and then appended to a compressed output stream in IR form.
See [README-protocol.md](README-protocol.md) for more details on the format of
CLP IR.

These log files containing the compressed CLP IR streams can then all be
ingested into CLP together at a later time.

## Quick Start

The package is hosted with pypi (https://pypi.org/project/clp-logging/), so it
can be installed with `pip`:

`python3 -m pip install --upgrade clp-logging`

## Logger handlers

### CLPStreamHandler

- Writes encoded logs directly to a stream

### CLPFileHandler

- Simple wrapper around CLPStreamHandler that calls open

#### Example: CLPFileHandler

```python
import logging
from pathlib import Path
from clp_logging.handlers import CLPFileHandler

clp_handler = CLPFileHandler(Path("example.clp.zst"))
logger = logging.getLogger(__name__)
logger.addHandler(clp_handler)
logger.warn("example warning")
```

### CLPSockHandler + CLPSockListener

This library also supports multiple processes writing to the same log file.
In this case, all logging processes write to a listener server process through a TCP socket.
The socket name is the log file path passed to CLPSockHandler with a ".sock" suffix.

A CLPSockListener can be explicitly created (and will run as a daemon) by calling:
 `CLPSockListener.fork(log_path, sock_path, timezone, timestamp_format)`.
Alternatively CLPSockHandlers can transparently start an associated CLPSockListener
by calling `CLPSockHandler` with `create_listener=True`.

CLPSockListener must be explicitly stopped once logging is completed.
There are two ways to stop the listener process:

- Calling `stop_listener()` from an existing handler, e.g., `clp_handler.stop_listener()`, or from a new handler with the same log path, e.g., `CLPSockHandler(Path("example.clp.zst")).stop_listener()`
- Kill the CLPSockListener process with SIGTERM

#### Example: CLPSockHandler + CLPSockListener

In the handler processes or threads:

```python
import logging
from pathlib import Path
from clp_logging.handlers import CLPSockHandler

clp_handler = CLPSockHandler(Path("example.clp.zst"), create_listener=True)
logger = logging.getLogger(__name__)
logger.addHandler(clp_handler)
logger.warn("example warning")
```

In a single process or thread once logging is completed:

```python
from pathlib import Path
from clp_logging.handlers import CLPSockHandler

CLPSockHandler(Path("example.clp.zst")).stop_listener()
```

## CLP readers (decoders)

### CLPStreamReader

- Read/decode any arbitrary stream
- Can be used as an iterator that returns each log message as an object
- Can skip n logs: `clp_reader.skip_nlogs(N)`
- Can skip to first log after given time (since unix epoch):
  - `clp_reader.skip_to_time(TIME)`

### CLPFileReader

- Simple wrapper around CLPStreamHandler that calls open

#### Example code: CLPFileReader

```python
from pathlib import Path
from typing import List

from clp_logging.readers import CLPFileReader, Log

# create a list of all Log objects
log_objects: List[Log] = []
with CLPFileReader(Path("example.clp.zst")) as clp_reader:
    for log in clp_reader:
        log_objects.append(log)
```

### CLPSegmentStreaming

* Classes inheriting CLPBaseReader are only capable of reading a single CLP IR stream from start to finish. This is required as to know the timestamp of an individual log, the starting timestamp (from the IR stream preamble) and all timestamp deltas up to that log must be known. In scenarios where a IR stream is periodically uploaded in chunks, users would need to either continuously read the entire stream or re-read the entire stream from the start.
* The CLPSegmentStreaming class has the ability to take an input IR stream and segment it, outputting multiple independent IR streams. This makes it possible to read arbitrary segments of the original input IR stream without needing to decode it from the start.
* In technical terms, the segment streaming reader allows the read operation to start from a non-zero offset, and streams the legal encoded logs from one stream to another.
* Each read call will return an encoded metadata which can be used to resume from the current call.S

#### Example code: CLPSegmentStreaming

```python
from clp_logging.readers import CLPSegmentStreaming
from clp_logging.protocol import Metadata

segment_idx: int = 0
segment_max_size: int = 8192
offset: int = 0
metadata: Metadata = None
while True:
	bytes_read: int
	with open("example.clp", "rb") as fin, open(f"{segment_idx}.clp", "wb") as fout:
		bytes_read, metadata = CLPSegmentStreaming.read(
			fin,
			fout,
			offset=offset,
			max_bytes_to_write=segment_max_size,
			metadata=metadata
		)
		segment_idx += 1
		offset += bytes_read
	if metadata == None:
		break
```

In the example code provided, "example.clp" is streamed into segments named "0.clp", "1.clp", and so on. Each segment is smaller than 8192 bytes and can be decoded independently.

## Log level timeout feature: CLPLogLevelTimeout

All log handlers support a configurable timeout feature. A (user configurable)
timeout will be scheduled based on logs' logging level (verbosity) that will
flush the zstandard frame and allow users to run arbitrary code.
This feature allows users to automatically perform log related tasks, such as
periodically uploading their logs for storage. By setting the timeout in
response to the logs' logging level the responsiveness of a task can be
adjusted based on the severity of logging level seen.
An additional timeout is always triggered on closing the logging handler.

See the class documentation for specific details.

#### Example code: CLPLogLevelTimeout

```python
import logging
from pathlib import Path
from clp_logging.handlers import CLPLogLevelTimeout, CLPSockHandler

class LogTimeoutUploader:
    # Store relevent information/objects to carry out the timeout task
    def __init__(self, log_path: Path) -> None:
        self.log_path: Path = log_path
        return

    # Create any methods necessary to carry out the timeout task
    def upload_log(self) -> None:
        # upload the logs to the cloud
        return

    def timeout(self) -> None:
        self.upload_log()

log_path: Path = Path("example.clp.zst")
uploader = LogTimeoutUploader(log_path)
loglevel_timeout = CLPLogLevelTimeout(uploader.timeout)
clp_handler = CLPSockHandler(log_path, create_listener=True, loglevel_timeout=loglevel_timeout)
logging.getLogger(__name__).addHandler(clp_handler)
```

## Compatibility

Tested on Python 3.6 and 3.8 and should work on any newer version.
Built/packaged on Python 3.8 for convenience regarding type annotation.

## Building/Packaging

1. Create and enter a virtual environment:
   `python3.8 -m venv venv; . ./venv/bin/activate`
2. Install development dependencies:
   `pip install -r requirements-dev.txt`
3. Build:
   `python -m build`

## Testing

Note the baseline comparison logging handler and the CLP handler both get
unique timestamps. It is possible for these timestamps to differ, which will
result in a test reporting a false positive error.

1. Create and enter a virtual environment:
   `python -m venv venv; .  ./venv/bin/activate`
2. Install:
   `pip install dist/clp_logging-*-py3-none-any.whl` or `pip install -e .`
3. Run unittest:
   `python -m unittest -bv`

## Contributing

Ensure to run `mypy` and `black` (found in
[requirements-dev.txt](requirements-dev.txt)) during development to ensure
smooth pull requests.

[0]: https://github.com/y-scope/clp
[1]: https://www.uber.com/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/
[2]: https://github.com/y-scope/yscope-log-viewer
