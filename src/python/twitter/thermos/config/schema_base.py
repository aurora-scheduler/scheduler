from pystachio import (
  Boolean,
  Default,
  Empty,
  Float,
  Integer,
  List,
  Map,
  Required,
  String,
  Struct
)


# Define constants for resources
BYTES = 1
KB = 1024 * BYTES
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB


class ThermosContext(Struct):
  # TODO(wickman) Move the underlying replacement mechanism to %port% replacements
  ports   = Map(String, Integer)

  # TODO(wickman) Move the underlying replacement mechanism to %task_id%
  task_id = String

  # TODO(wickman) Move underlying mechanism to %user%
  user    = String


class Resources(Struct):
  cpu  = Required(Float)
  ram  = Required(Integer)
  disk = Required(Integer)


class Constraint(Struct):
  order = List(String)


class Process(Struct):
  cmdline = Required(String)
  name    = Required(String)

  # This is currently unused but reserved for future use by Thermos.
  resources     = Resources

  # optionals
  max_failures  = Default(Integer, 1)      # maximum number of failed process runs
                                           # before process is failed.
  daemon        = Default(Boolean, False)
  ephemeral     = Default(Boolean, False)
  min_duration  = Default(Integer, 5)      # integer seconds
  final         = Default(Boolean, False)  # if this process should be a finalizing process
                                           # that should always be run after regular processes


class Task(Struct):
  name = Default(String, '{{processes[0].name}}')
  processes = List(Process)

  # optionals
  constraints = Default(List(Constraint), [])
  resources = Resources
  max_failures = Default(Integer, 1)        # maximum number of failed processes before task is failed.
  max_concurrency = Default(Integer, 0)     # 0 is infinite concurrency.
                                            # > 0 is max concurrent processes.
  finalization_wait = Default(Integer, 30)  # the amount of time in seconds we allocate to run the
                                            # finalization schedule.

  # TODO(jon): remove/replace with proper solution to MESOS-3546
  user = String
