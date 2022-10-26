# Overview
CLP's intermediate representation (IR) is made to be:
    - Capable of losslessly encoding and compressing unstructured logs at log
      event granularity
    - Capable of flushing write buffers on demand so readers can access the
      latest data

See [encoder.py](src/clp_logging/encoder.py) and
[decoder.py](src/clp_logging/decoder.py) to see the implementation.

# Background
CLP parses a log event into zero or more variables, a timestamp, and a logtype.
There are two major types of variables:
- Variables which can be converted from a string into an encoded form (e.g., an
  int)
- Variables which cannot be converted from a string (these will be stored in a
  dictionary during CLP’s compression, so we refer to them as dictionary
  variables)
CLP also needs to store some basic metadata like the timezone ID and timestamp
format.

# Basics
The protocol writes a stream of bytes with the following structure:
- /magic number/ - Which CLP stream encoding protocol is used. Can be standard
  or compact (an optimization described later)
- /metadata/
- /variable(s), logtype, and timestamp//variable(s), logtype, and timestamp/...
- EOF

At a high-level, the stream is written as follows:
- The encoding type is written in the form: /4 bytes/
- The metadata is written in the form: /metadata encoding tag
byte//serialized metadata/
- Each non-dictionary variable in the form: /tag byte//encoded value/
- Each dictionary variable in the form: /tag byte//length//content/
- Each logtype in the form: /tag byte//length//content/
- Each non-null timestamp in the form: /tag byte//timestamp value/
- Each null timestamp is in the form: /tag byte/
