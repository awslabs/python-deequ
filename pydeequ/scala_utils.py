# -*- coding: utf-8 -*-
"""A collection of utility functions and classes for manipulating with scala objects anc classes through py4j
"""
from py4j.java_gateway import JavaObject


class PythonCallback:
    # https://www.py4j.org/advanced_topics.html#py4j-memory-model
    # https://stackoverflow.com/questions/50878834/py4j-error-while-obtaining-a-new-communication-channel-on-multithreaded-java
    def __init__(self, gateway):
        self.gateway = gateway
        # P4j will return false if the callback server is already started
        # https://github.com/bartdag/py4j/blob/master/py4j-python/src/py4j/java_gateway.py
        callback_server = self.gateway.get_callback_server()
        # TODO clean
        if callback_server is None:
            self.gateway.start_callback_server()
            print("Python Callback server started!")  # TODO Logging
        elif callback_server.is_shutdown:
            callback_server.close()
            self.gateway.restart_callback_server()
            # Have you tried turning it off and on again?
            # TODO why do we need to restart this every time?
            # TODO Will this break during chained function calls?
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
