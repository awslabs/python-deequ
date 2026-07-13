# -*- coding: utf-8 -*-
"""A collection of utility functions and classes for manipulating with scala objects anc classes through py4j
"""
from py4j.java_gateway import DEFAULT_PYTHON_PROXY_PORT, JavaObject


def _ensure_dynamic_callback_port(gateway):
    """Make sure the py4j callback server will NOT bind the hardcoded default
    port (25334) before it is started.

    Historically PyDeequ called ``gateway.start_callback_server()`` with no
    arguments. With older py4j/pyspark gateways that left the callback server
    on the hardcoded default port ``DEFAULT_PYTHON_PROXY_PORT`` (25334), so two
    pyspark applications -- or two runs on the same host -- using a lambda-based
    ``Check`` collided with
    ``OSError: [Errno 98] Address already in use (127.0.0.1:25334)``
    (issues #86, #19, #7, #72, #156, #173, #198).

    The fix is simply to ask the OS for a free port (``port = 0``). We mutate
    the gateway's EXISTING ``callback_server_parameters`` rather than passing a
    fresh ``CallbackServerParameters`` object, so PySpark's own callback wiring
    is preserved -- the JVM-side callback client stays consistent (no "Error
    while obtaining a new communication channel", #19) and, crucially,
    ``shutdown_callback_server()`` still returns cleanly at teardown. Passing a
    fresh parameters object + ``resetCallbackClient`` instead makes the accept
    thread un-joinable and hangs shutdown on Linux and macOS, which would hang
    the test suite's ``tearDownClass``.
    """
    params = getattr(gateway, "callback_server_parameters", None)
    if params is not None and getattr(params, "port", 0) == DEFAULT_PYTHON_PROXY_PORT:
        params.port = 0


class PythonCallback:
    # https://www.py4j.org/advanced_topics.html#py4j-memory-model
    # https://stackoverflow.com/questions/50878834/py4j-error-while-obtaining-a-new-communication-channel-on-multithreaded-java
    def __init__(self, gateway):
        self.gateway = gateway
        # P4j will return false if the callback server is already started
        # https://github.com/bartdag/py4j/blob/master/py4j-python/src/py4j/java_gateway.py
        callback_server = self.gateway.get_callback_server()
        if callback_server is None:
            # No callback server yet: ensure a dynamic port (avoid the 25334
            # collision) and start it the stock way so PySpark's callback
            # wiring -- and clean shutdown -- are preserved.
            _ensure_dynamic_callback_port(self.gateway)
            self.gateway.start_callback_server()
            print("Python Callback server started!")  # TODO Logging
        elif callback_server.is_shutdown:
            # The previous callback server was shut down (e.g. a prior Spark
            # session was stopped). Restart it (also dynamic-port safe).
            callback_server.close()
            _ensure_dynamic_callback_port(self.gateway)
            self.gateway.restart_callback_server()
            print("PythonCallback server restarted!")


class ScalaFunction1(PythonCallback):
    """Implements scala.Function1 interface so we can pass lambda functions to Check
    https://www.scala-lang.org/api/current/scala/Function1.html"""

    def __init__(self, gateway, lambda_function):
        super().__init__(gateway)
        self.lambda_function = lambda_function

    def apply(self, arg):
        """Implements the apply function"""
        return self.lambda_function(arg)

    class Java:
        """scala.Function1: a function that takes one argument"""

        implements = ["scala.Function1"]


class ScalaFunction2(PythonCallback):
    """Implements scala.Function2 interface
    https://www.scala-lang.org/api/current/scala/Function2.html"""

    def __init__(self, gateway, lambda_function):
        super().__init__(gateway)
        self.lambda_function = lambda_function

    def apply(self, t1, t2):
        """Implements the apply function"""
        return self.lambda_function(t1, t2)

    class Java:
        """scala.Function2: a function that takes two arguments"""

        implements = ["scala.Function2"]


def get_or_else_none(scala_option):
    # TODO Checks
    if scala_option.isEmpty():
        return None
    return scala_option.get()


def to_scala_seq(jvm, iterable):
    """
    Helper method to take an iterable and turn it into a Scala sequence
    Args:
        jvm: Spark session's JVM
        iterable: your iterable
    Returns:
        Scala sequence
    """
    return jvm.scala.collection.JavaConversions.iterableAsScalaIterable(iterable).toSeq()


def to_scala_map(spark_session, d):
    """
    Convert a dict into a JVM Map.
    Args:
        spark_session: Spark session
        d: Python dictionary
    Returns:
        Scala map
    """
    return spark_session._jvm.PythonUtils.toScalaMap(d)


def scala_map_to_dict(jvm, scala_map):
    return dict(jvm.scala.collection.JavaConversions.mapAsJavaMap(scala_map))


def scala_map_to_java_map(jvm, scala_map):
    return jvm.scala.collection.JavaConversions.mapAsJavaMap(scala_map)


def java_list_to_python_list(java_list: str, datatype):
    # TODO: Improve
    # Currently turn the java object into a string and pass it through
    start = java_list.index("(")
    end = java_list.index(")")
    empty_val = "" if datatype == str else None
    return [datatype(i) if i != "" else empty_val for i in java_list[start + 1 : end].split(",")]


def scala_get_default_argument(java_object, argument_idx: int) -> JavaObject:
    return getattr(java_object, f"apply$default${argument_idx}")()
