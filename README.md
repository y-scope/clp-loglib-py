# CLP Python Logging Library

This is a Python `logging` library meant to supplement [CLP (Compressed Log Processor)][0].
It operates by serializing and compressing log events using the CLP Intermediate Representation (IR)
format, achieving both data streaming capabilities and effective compression ratios. Log files
serialized using the IR format can be viewed using the [YScope Log Viewer][2]. They can also be deserialized
to their original plain-text format, or programmatically analyzed with the APIs provided by
[clp-ffi-py][9]. For further information, refer to the detailed explanation in this [Uber blog][1].

## Motivation

CLP buffers a substantial volume of log files before executing compression for a better compression
ratio. However, most individual log files are actively opened for appending over an extended
duration. In their raw-text format, these log files are not space-efficient and do not support
efficient querying through standard text-based tools like `grep`.

To address this problem, this logging library is designed to serialize log events directly in CLP's
Intermediate Representation (IR) format. A log event created with a CLP logging handler will first
be encoded into the IR format, and then appended to a compressed output stream. This approach not
only minimizes storage resource consumption but also facilitates the execution of high-performance,
early-stage analytics using the APIs from [clp-ffi-py][9]. These compressed CLP IR files can be
further processed by CLP to achieve superior compression ratios and more extensive analytics
capabilities.

For a detailed understanding of the CLP IR format, refer to [README-protocol.md](README-protocol.md)

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

> [!WARNING]
> The readers and all the other non-logging APIs currently available in this library are scheduled
> for deprecation in an upcoming release. To access our newest and improved CLP IR analytics
> interface (which offers advanced features like high-performance decoding and enhanced query search
> capabilities) check out [clp-ffi-py][9].

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

* Classes that inherit from CLPBaseReader can only read a single CLP IR stream from start to finish. This is necessary because, to determine the timestamp of an individual log, the starting timestamp (from the IR stream preamble) and all timestamp deltas up to that log must be known. In scenarios where an IR stream is periodically uploaded in chunks, users would need to either continuously read the entire stream or re-read the entire stream from the start.
* The CLPSegmentStreaming class has the ability to take an input IR stream and segment it, outputting multiple independent IR streams. This makes it possible to read arbitrary segments of the original input IR stream without needing to decode it from the start.
* In technical terms, the segment streaming reader allows the read operation to start from a non-zero offset and streams the legally encoded logs from one stream to another.
* Each read call will return encoded metadata that can be used to resume from the current call.

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

Tested on Python 3.7, 3.8, and 3.11 (should also work on newer versions).
Built/packaged on Python 3.8 for convenience regarding type annotation.

## Development

### Setup environment

1. Create and enter a virtual environment:
   `python3.8 -m venv venv; . ./venv/bin/activate`
2. Install the project in editable mode, the development dependencies, and the test dependencies:
   `pip install -e .[dev,test]`

Note: you may need to upgrade pip first for `-e` to work. If so, run: `pip install --upgrade pip`

### Packaging

To build a package for distribution run:
   `python -m build`

### Testing

To run the unit tests run:
   `python -m unittest -bv`

Note: the baseline comparison logging handler and the CLP handler both get
unique timestamps. It is possible for these timestamps to differ, which will
result in a test reporting a false positive error.

## Contributing

Before submitting a pull request, run the following error-checking and
formatting tools (found in [requirements-dev.txt](requirements-dev.txt)):

* [mypy][3]: `mypy src tests`
  * mypy checks for typing errors. You should resolve all typing errors or if an
    error cannot be resolved (e.g., it's due to a third-party library), you
    should add a comment `# type: ignore` to [silence][4] the error.
* [docformatter][5]: `docformatter -i src tests`
  * This formats docstrings. You should review and add any changes to your PR.
* [Black][6]: `black src tests`
  * This formats the code according to Black's code-style rules. You should
    review and add any changes to your PR.
* [ruff][8]: `ruff check --fix src tests`
  * This performs linting according to PEPs. You should review and add any
    changes to your PR.

Note that `docformatter` should be run before `black` to give Black the [last
word][7].

[0]: https://github.com/y-scope/clp
[1]: https://www.uber.com/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/
[2]: https://github.com/y-scope/yscope-log-viewer
[3]: https://mypy.readthedocs.io/en/stable/index.html
[4]: https://mypy.readthedocs.io/en/stable/common_issues.html#spurious-errors-and-locally-silencing-the-checker
[5]: https://docformatter.readthedocs.io/en/latest/
[6]: https://black.readthedocs.io/en/stable/index.html
[7]: https://docformatter.readthedocs.io/en/latest/faq.html#interaction-with-black
[8]: https://beta.ruff.rs/docs/
[9]: https://github.com/y-scope/clp-ffi-py
