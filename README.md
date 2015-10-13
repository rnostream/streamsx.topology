# Python Application API
The Python Application Api (PAA) enables developers to write IBM Streams applications using only python code.

## Requirements
The Python Application API requires Python >= 3.4.1, and a release of the streamsx.topology project built using the 'python' branch.

## Setup
In your .bashrc, ensure that Python can find the required modules, and that the Python shared libraries can be resolved:

```bash
export PYTHONPATH=~/git/streamsx.topology/com.ibm.streamsx.topology/opt/python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<path to python install>/Python-3.4.3
```
## Usage

### (Optional) Tab completion
To use the PAA, as a convenience (optionally) enable tab completion in the Python interpreter to avoid typing out long module names:

``` python
import rlcompleter, readline
readline.parse_and_bind('tab: complete')
```

### Hello World
To begin, start the Python 3.4 REPL:
``` bash
$ python3.4
```

At the prompt, import the required modules:
``` python
>>> from topology.Topology import *
>>> from topology.context.StreamsContextFactory import *
```

Then, create the topology object that will contain the structure and data sources of your application.

``` python
>>> t = Topology("HelloWorldTopology")
```

To create a stream of string tuples, simply invoke the topology's string() method:
``` python
>>> hw = t.strings(["Hello", " world!"])
```

Then, simply print the stream to standard output:
``` python
>>> hw.printStream()
```

To run the application, submit the topology to a STANDALONE submission context:
``` python
>>> StreamsContextFactory.get_streams_context(Types.STANDALONE).submit(t)
```

after the topology compiles, verify that the output  is:
```
Hello
 world!
```

### Passing user-defined classes/modules as tuples
If our current working directory is ~/py, let's say we have the following module in ~/py/UserDefinedClass/ComplexNumber.py:
``` python
import math

class ComplexNumber(object):
    """This is an example of a user-provided class that might be passed as a tuple through a stream"""
    def __init__(self, real = 0.0, complex = 0.0):
        self.real = real
        self.complex = complex

    def magnitude(self):
        """Returns the magnitude of the vector on the complex plane"""
        return math.sqrt(math.pow(self.real, 2) + math.pow(self.complex, 2))
```

ComplexNumber has a real component, an imaginary component, and a method for returning the magnitude of the point on the complex plane. We want to pass ComplexNumbers as tuples on a stream. Fortunately, the Python Application has support for including third party modules into the application's runtime. To do this, when declaring your topology object, pass in the location of the ComplexNumber module (if running from a file, also include the location of the main module, in this case 'tester.py'):
``` python
top = Topology("pythonClassTopology", ["UserDefinedClass", "tester.py"])
```

Great, now the module is ready to be used. Here is an example of using the module in a PAA application:
``` python
from topology.Topology import Topology
from UserDefinedClass.ComplexNumber import ComplexNumber
from topology.context.StreamsContextFactory import StreamsContextFactory, Types

def complexNumberCreator(x):
    return ComplexNumber(x + x/2, x - x/2)

if __name__ == "__main__":
    """This example demonstrates how third-party classes can be passed as a tuple on a stream."""
    top = Topology("pythonClassTopology", ["UserDefinedClass", "tester.py"])
    count = top.counter()
    complex_stream = count.transform(complexNumberCreator)
    conjugate_stream = complex_stream.transform(lambda r: r.magnitude())
    conjugate_stream.printStream()
    StreamsContextFactory.get_streams_context(Types.STANDALONE).submit(top)
```

Note that Toplogy.counter() is a utility method that returns a stream containing 1, 2, 3, ...
Furthermore, note that in the transform function, a lambda may be specified.
Now, run the application, and verify that the ouput is a continuously generated list of floating point number magnitudes!

``` bash
$ python3.4 tester.py
```

output:
``` 
18069.25455020212
18070.835689032203
18072.41682786229
18073.997966692372
18075.579105522454
18077.16024435254
18078.741383182623
18080.32252201271
18081.903660842792
18083.48479967288
18085.06593850296
...
```

# streamsx.topology
A project that supports building streaming topologies (applications)
for IBM Streams in different programming languages, such as Java and Scala.
http://ibmstreams.github.io/streamsx.topology/

## Java Application API
The Java Application API enables a developer to create streaming applications entirely in Java for IBM Streams. The API employs a functional style of programming -- a developer may define a graph's flow and data manipulation simultaneously.

Please refer to the [getting started guide](http://ibmstreams.github.io/streamsx.topology/gettingstarted.html), [FAQ page](http://ibmstreams.github.io/streamsx.topology/FAQ.html), and [documentation](http://ibmstreams.github.io/streamsx.topology/doc.html) for help.

## Scala Support
Scala support enables a developer to create streaming applications entirely in Scala for IBM Streams. Scala support requires the application calls into the Java Application API (as Java & Scala are both JVM languages), and includes implicit conversions to allow Scala anonymous functions to be used as the functional transformations.

Please see this initial documentation: https://github.com/IBMStreams/streamsx.topology/wiki/Scala-Support
