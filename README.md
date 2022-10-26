# CLP Logging
This is a Python `logging` library meant to supplement CLP. To learn more about
CLP check out the [repository][0] and [Uber's blog][1].

To achieve the best compression ratio, CLP should be used to compress large
batches of related log events, one batch at a time. However, individual log
files are generally small and are generated across a long period of time.

This logging library helps solve this problem by logging directly in CLP's
internal representation (IR). A log created with a CLP logging handler is first
parsed and then appended to a compressed output stream in IR form.
See [README-protocol.md](README-protocol.md) for more details on the format of
CLP IR.

These log files containing the compressed CLP IR streams can then all be
ingested into CLP together at a later time.

[0]: https://github.com/y-scope/clp
[1]: https://www.uber.com/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/

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
- A Unix domain socket logging handler and listener server to enable multiple
  concurrent handlers across multiple processes to log to the same log file.
- The handler writes encoded logs and sends them over the socket to the
  separate listener process
    - Socket name is the log file path passed to CLPSockHandler with a ".sock"
      suffix
- On creation a CLPSockHandler will try to connect to the listener associated
  with its socket name
- CLPSockListener is essentially a namespace as it is a class, but only
  contains static methods and data
- A CLPSockListener can be explicitly created (and will run as a daemon) by
  calling:
    - `CLPSockListener.fork(log_path, sock_path, timezone, timestamp_format)`
- Alternatively CLPSockHandlers can transparently start an associated
  CLPSockListener
    - Call `CLPSockHandler` with `create_listener=True`
- Multiple CLPSockHandlers logging to the same file will use the same socket
  and therefore the same listener
    - It is safe for these handlers to all use `create_listener=True`
        - They will race to create a listener with only one successfully
          binding the socket and living on
        - All the handlers will then connect to the one successful listener
- CLPSockListener must be explicitly stopped once logging is completed
    - Send CLPSockListener process SIGTERM
    - Use an existing handler or create a new handler with the same log path
      and call `stop_listener`
        - `clp_sock_handler.stop_listener()` or
          `CLPSockHandler(Path("example.clp.zst")).stop_listener()`

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
import logging
from pathlib import Path
from clp_logging.readers import CLPFileReader, Log

# create a list of all Log objects
log_objects: List[Log] = []
with CLPFileReader(Path("example.clp.zst") as clp_reader:
    for log in clp_reader:
        log_objects.append(log)
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
    `pip install dist/clp_logging-0.0.1-py3-none-any.whl` or `pip install -e .`
3. Run unittest:
    `python -m unittest -bv`

## Contributing
Ensure to run `mypy` and `black` (found in
[requirements-dev.txt](requirements-dev.txt)) during development to ensure
smooth pull requests.
