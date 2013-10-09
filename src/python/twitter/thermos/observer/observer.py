"""Observe Thermos tasks on a system

This module provides a number of classes for exposing information about running (active) and
finished Thermos tasks on a system. The primary entry point is the TaskObserver, a thread which
polls a designated Thermos checkpoint root and collates information about all tasks it discovers.

"""
import os
import errno
import time
import json
import urllib
import threading
from abc import abstractmethod, abstractproperty
from collections import defaultdict
from operator import attrgetter

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.lang import AbstractClass, Lockable
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader

from twitter.thermos.monitoring.detector import TaskDetector
from twitter.thermos.monitoring.monitor import TaskMonitor
from twitter.thermos.monitoring.process import ProcessSample
from twitter.thermos.monitoring.resource import ResourceMonitorBase, TaskResourceMonitor

from twitter.thermos.base.path import TaskPath
from twitter.thermos.base.ckpt import CheckpointDispatcher

from pystachio import Environment
from twitter.thermos.config.schema import ThermosContext
from twitter.thermos.config.loader import (
  ThermosTaskWrapper,
  ThermosProcessWrapper)

from gen.twitter.thermos.ttypes import *


def safe_mtime(path):
  try:
    return os.path.getmtime(path)
  except OSError:
    return None


class ObservedTask(AbstractClass):
  """ Represents a Task being observed """
  def __init__(self, task_id, pathspec):
    self._task_id = task_id
    self._pathspec = pathspec
    self._mtime = self._get_mtime()

  @abstractproperty
  def type(self):
    """Indicates the type of task (active or finished)"""

  def _read_task(self, memoized={}):
    """Read the corresponding task from disk and return a ThermosTask.  Memoizes already-read tasks.
    """
    if self._task_id not in memoized:
      path = self._pathspec.given(task_id=self._task_id, state=self.type).getpath('task_path')
      if os.path.exists(path):
        task = ThermosTaskWrapper.from_file(path)
        if task is None:
          log.error('Error reading ThermosTask from %s in observer.' % path)
        else:
          context = self.context(self._task_id)
          if not context:
            log.warning('Task not yet available: %s' % self._task_id)
          task = task.task() % Environment(thermos=context)
          memoized[self._task_id] = task

    return memoized.get(self._task_id, None)

  def _get_mtime(self):
    """Retrieve the mtime of the task's state directory"""
    get_path = lambda state: self._pathspec.given(
      task_id=self._task_id, state=state).getpath('task_path')
    mtime = safe_mtime(get_path('active'))
    if mtime is None:
      mtime = safe_mtime(get_path('finished'))
    if mtime is None:
      log.error("Couldn't get mtime for task %s!" % self._task_id)
    return mtime

  def context(self, task_id):
    state = self.state
    if state.header is None:
      return None
    return ThermosContext(
      ports = state.header.ports if state.header.ports else {},
      task_id = state.header.task_id,
      user = state.header.user,
    )

  @property
  def task(self):
    """Return a ThermosTask representing this task"""
    return self._read_task()

  @property
  def task_id(self):
    """Return the task's task_id"""
    return self._task_id

  @property
  def mtime(self):
    """Return mtime of task file"""
    return self._mtime

  @abstractproperty
  def state(self):
    """Return state of task (gen.twitter.thermos.ttypes.RunnerState)"""


class ActiveObservedTask(ObservedTask):
  """An active Task known by the TaskObserver"""
  def __init__(self, task_id, pathspec, task_monitor, resource_monitor):
    super(ActiveObservedTask, self).__init__(task_id, pathspec)
    self._task_monitor = task_monitor
    self._resource_monitor = resource_monitor

  @property
  def type(self):
    return 'active'

  @property
  def state(self):
    """Return a RunnerState representing the current state of task, retrieved from TaskMonitor"""
    return self.task_monitor.get_state()

  @property
  def task_monitor(self):
    """Return a TaskMonitor monitoring this task"""
    return self._task_monitor

  @property
  def resource_monitor(self):
    """Return a ResourceMonitor implementation monitoring this task's resources"""
    return self._resource_monitor


class FinishedObservedTask(ObservedTask):
  """A finished Task known by the TaskObserver"""
  def __init__(self, task_id, pathspec):
    super(FinishedObservedTask, self).__init__(task_id, pathspec)
    self._state = None

  @property
  def type(self):
    return 'finished'

  @property
  def state(self):
    """Return final state of Task (RunnerState, read from disk and cached for future access)"""
    if self._state is None:
      path = self._pathspec.given(task_id=self._task_id).getpath('runner_checkpoint')
      self._state = CheckpointDispatcher.from_file(path)
    return self._state



class TaskObserver(ExceptionalThread, Lockable):
  """
    The TaskObserver monitors the thermos checkpoint root for active/finished
    tasks.  It is used to be the oracle of the state of all thermos tasks on
    a machine.

    It currently returns JSON, but really should just return objects.  We should
    then build an object->json translator.
  """
  class UnexpectedError(Exception): pass
  class UnexpectedState(Exception): pass

  POLLING_INTERVAL = Amount(1, Time.SECONDS)

  def __init__(self, root, resource_monitor_class=TaskResourceMonitor):
    self._pathspec = TaskPath(root=root)
    self._detector = TaskDetector(root)
    if not issubclass(resource_monitor_class, ResourceMonitorBase):
      raise ValueError("resource monitor class must implement ResourceMonitorBase!")
    self._resource_monitor = resource_monitor_class
    self._active_tasks = {}    # task_id => ActiveObservedTask
    self._finished_tasks = {}  # task_id => FinishedObservedTask
    self._stop_event = threading.Event()
    ExceptionalThread.__init__(self)
    Lockable.__init__(self)
    self.daemon = True

  @property
  def active_tasks(self):
    """Return a dictionary of active Tasks"""
    return self._active_tasks

  @property
  def finished_tasks(self):
    """Return a dictionary of finished Tasks"""
    return self._finished_tasks

  @property
  def all_tasks(self):
    """Return a dictionary of all Tasks known by the TaskObserver"""
    return dict(self.active_tasks.items() + self.finished_tasks.items())

  def stop(self):
    self._stop_event.set()

  def start(self):
    ExceptionalThread.start(self)

  @Lockable.sync
  def add_active_task(self, task_id):
    if task_id in self.finished_tasks:
      log.error('Found an active task (%s) in finished tasks?' % task_id)
      return
    task_monitor = TaskMonitor(self._pathspec, task_id)
    if not task_monitor.get_state().header:
      log.info('Unable to load task "%s"' % task_id)
      return
    sandbox = task_monitor.get_state().header.sandbox
    resource_monitor = self._resource_monitor(task_monitor, sandbox)
    resource_monitor.start()
    self._active_tasks[task_id] = ActiveObservedTask(
      task_id=task_id, pathspec=self._pathspec,
      task_monitor=task_monitor, resource_monitor=resource_monitor
    )

  @Lockable.sync
  def add_finished_task(self, task_id):
    self._finished_tasks[task_id] = FinishedObservedTask(
      task_id=task_id, pathspec=self._pathspec
    )

  @Lockable.sync
  def active_to_finished(self, task_id):
    self.remove_active_task(task_id)
    self.add_finished_task(task_id)

  @Lockable.sync
  def remove_active_task(self, task_id):
    task = self.active_tasks.pop(task_id)
    task.resource_monitor.kill()

  @Lockable.sync
  def remove_finished_task(self, task_id):
    self.finished_tasks.pop(task_id)

  def run(self):
    """
      The internal thread for the observer.  This periodically polls the
      checkpoint root for new tasks, or transitions of tasks from active to
      finished state.
    """
    while not self._stop_event.is_set():
      time.sleep(self.POLLING_INTERVAL.as_(Time.SECONDS))

      active_tasks = [task_id for _, task_id in self._detector.get_task_ids(state='active')]
      finished_tasks = [task_id for _, task_id in self._detector.get_task_ids(state='finished')]

      with self.lock:

        # Ensure all tasks currently detected on the system are observed appropriately
        for active in active_tasks:
          if active not in self.active_tasks:
            log.debug('task_id %s (unknown) -> active' % active)
            self.add_active_task(active)
        for finished in finished_tasks:
          if finished in self.active_tasks:
            log.debug('task_id %s active -> finished' % finished)
            self.active_to_finished(finished)
          elif finished not in self.finished_tasks:
            log.debug('task_id %s (unknown) -> finished' % finished)
            self.add_finished_task(finished)

        # Remove ObservedTasks for tasks no longer detected on the system
        for unknown in set(self.active_tasks) - set(active_tasks + finished_tasks):
          log.debug('task_id %s active -> (unknown)' % unknown)
          self.remove_active_task(unknown)
        for unknown in set(self.finished_tasks) - set(active_tasks + finished_tasks):
          log.debug('task_id %s finished -> (unknown)' % unknown)
          self.remove_finished_task(unknown)


  @Lockable.sync
  def process_from_name(self, task_id, process_id):
    if task_id in self.all_tasks:
      task = self.all_tasks[task_id].task
      if task:
        for process in task.processes():
          if process.name().get() == process_id:
            return process

  @Lockable.sync
  def task_count(self):
    """
      Return the count of tasks that could be ready properly from disk.
      This may be <= self.task_id_count()
    """
    return dict(
      active = len(self.active_tasks),
      finished = len(self.finished_tasks),
      all = len(self.all_tasks),
    )

  @Lockable.sync
  def task_id_count(self):
    """
      Return the raw count of active and finished task_ids from the TaskDetector.
    """
    num_active = len(list(self._detector.get_task_ids(state='active')))
    num_finished = len(list(self._detector.get_task_ids(state='finished')))
    return dict(
      active = num_active,
      finished = num_finished,
      all = num_active + num_finished,
    )

  def _get_tasks_of_type(self, type):
    """Convenience function to return all tasks of a given type"""
    tasks = {
      'active':   self.active_tasks,
      'finished': self.finished_tasks,
      'all':      self.all_tasks,
    }.get(type, None)

    if tasks is None:
      log.error('Unknown task type %s' % type)
      return {}

    return tasks

  @Lockable.sync
  def state(self, task_id):
    """Return a dict containing mapped information about a task's state"""
    real_state = self.raw_state(task_id)
    if real_state is None or real_state.header is None:
      return {}
    else:
      return dict(
        task_id = real_state.header.task_id,
        launch_time = real_state.header.launch_time_ms/1000.0,
        sandbox = real_state.header.sandbox,
        hostname = real_state.header.hostname,
        user = real_state.header.user
      )

  @Lockable.sync
  def raw_state(self, task_id):
    """
      Return the current runner state (thrift blob: gen.twitter.thermos.ttypes.RunnerState)
      of a given task id
    """
    if task_id not in self.all_tasks:
      return None
    return self.all_tasks[task_id].state

  @Lockable.sync
  def _task_processes(self, task_id):
    """
      Return the processes of a task given its task_id.

      Returns a map from state to processes in that state, where possible
      states are: waiting, running, success, failed.
    """
    if task_id not in self.all_tasks:
      return {}
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}

    waiting, running, success, failed, killed = [], [], [], [], []
    for process, runs in state.processes.items():
      # No runs ==> nothing started.
      if len(runs) == 0:
        waiting.append(process)
      else:
        if runs[-1].state in (None, ProcessState.WAITING, ProcessState.LOST):
          waiting.append(process)
        elif runs[-1].state in (ProcessState.FORKED, ProcessState.RUNNING):
          running.append(process)
        elif runs[-1].state == ProcessState.SUCCESS:
          success.append(process)
        elif runs[-1].state == ProcessState.FAILED:
          failed.append(process)
        elif runs[-1].state == ProcessState.KILLED:
          killed.append(process)
        else:
          # TODO(wickman)  Consider log.error instead of raising.
          raise TaskObserver.UnexpectedState(
            "Unexpected ProcessHistoryState: %s" % state.processes[process].state)

    return dict(
      waiting = waiting,
      running = running,
      success = success,
      failed = failed,
      killed = killed
    )

  @Lockable.sync
  def main(self, type=None, offset=None, num=None):
    """Return a set of information about tasks, optionally filtered

      Args:
        type = (all|active|finished|None) [default: all]
        offset = offset into the list of task_ids [default: 0]
        num = number of results to return [default: 20]

      Tasks are sorted by interest:
        - active tasks are sorted by start time
        - finished tasks are sorted by completion time

      Returns:
        {
          tasks: [task_id_1, ..., task_id_N],
          type: query type,
          offset: next offset,
          num: next num
        }

    """
    type = type or 'all'
    offset = offset or 0
    num = num or 20

    # Get a list of all ObservedTasks of requested type
    tasks = sorted((task for task in self._get_tasks_of_type(type).values()),
                   key=attrgetter('mtime'), reverse=True)

    # Filter by requested offset + number of results
    end = num
    if offset < 0:
      offset = offset % len(tasks) if len(tasks) > abs(offset) else 0
    end += offset
    tasks = tasks[offset:end]

#    # Gather filtered set of task_ids representing tasks of interest
#    task_map = self.task_ids(type, offset, num)
#    task_ids = task_map['task_ids']
#
#    tasks = dict((task_id, self._task(task_id)) for task_id in self._get_tasks_of_type(type)
#                  if task_id in task_ids)

    def task_row(observed_task):
      """Generate an output row for a Task"""
      task = self._task(observed_task.task_id)
      # tasks include those which could not be found properly and are hence empty {}
      if task:
        return dict(
            task_id=observed_task.task_id,
            name=task['name'],
            role=task['user'],
            launch_timestamp=task['launch_timestamp'],
            state=task['state'],
            state_timestamp=task['state_timestamp'],
            ports=task['ports'],
            **task['resource_consumption'])

    return dict(
      tasks=filter(None, map(task_row, tasks)),
      type=type,
      offset=offset,
      num=num,
      task_count=self.task_count()[type],
    )


  def _sample(self, task_id):
    if task_id not in self.active_tasks:
      log.debug("Task %s not found in active tasks" % task_id)
      sample = ProcessSample.empty().to_dict()
      sample['disk'] = 0
    else:
      resource_sample = self.active_tasks[task_id].resource_monitor.sample()[1]
      sample = resource_sample.process_sample.to_dict()
      sample['disk'] = resource_sample.disk_usage
      log.debug("Got sample for task %s: %s" % (task_id, sample))
    return sample

  @Lockable.sync
  def task_statuses(self, task_id):
    """
      Return the sequence of task states.

      [(task_state [string], timestamp), ...]
    """

    # Unknown task_id.
    if task_id not in self.all_tasks:
      return []

    task = self.all_tasks[task_id]
    if task is None:
      return []

    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return []

    # Get the timestamp of the transition into the current state.
    return [
      (TaskState._VALUES_TO_NAMES.get(state.state, 'UNKNOWN'), state.timestamp_ms / 1000)
      for state in state.statuses]


  @Lockable.sync
  def _task(self, task_id):
    """
      Return composite information about a particular task task_id, given the below
      schema.

      {
         task_id: string,
         name: string,
         user: string,
         launch_timestamp: seconds,
         state: string [ACTIVE, SUCCESS, FAILED]
         ports: { name1: 'url', name2: 'url2' }
         resource_consumption: { cpu:, ram:, disk: }
         processes: { -> names only
            waiting: [],
            running: [],
            success: [],
            failed:  []
         }
      }
    """
    # Unknown task_id.
    if task_id not in self.all_tasks:
      return {}

    task = self.all_tasks[task_id].task
    if task is None:
      # TODO(wickman)  Can this happen?
      log.error('Could not find task: %s' % task_id)
      return {}

    state = self.raw_state(task_id)
    if state is None or state.header is None:
      # TODO(wickman)  Can this happen?
      return {}

    # Get the timestamp of the transition into the current state.
    current_state = state.statuses[-1].state
    last_state = state.statuses[0]
    state_timestamp = 0
    for status in state.statuses:
      if status.state == current_state and last_state != current_state:
        state_timestamp = status.timestamp_ms / 1000
      last_state = status.state

    return dict(
       task_id = task_id,
       name = task.name().get(),
       launch_timestamp = state.statuses[0].timestamp_ms / 1000,
       state = TaskState._VALUES_TO_NAMES[state.statuses[-1].state],
       state_timestamp = state_timestamp,
       user = state.header.user,
       resource_consumption = self._sample(task_id),
       ports = state.header.ports,
       processes = self._task_processes(task_id),
       task_struct = task
    )

  @Lockable.sync
  def _get_process_resource_consumption(self, task_id, process_name):
    if task_id not in self.active_tasks:
      log.debug("Task %s not found in active tasks" % task_id)
      return ProcessSample.empty().to_dict()
    sample = self.active_tasks[task_id].resource_monitor.sample_by_process(process_name).to_dict()
    log.debug('Resource consumption (%s, %s) => %s' % (task_id, process_name, sample))
    return sample

  @Lockable.sync
  def _get_process_tuple(self, history, run):
    """
      Return the basic description of a process run if it exists, otherwise
      an empty dictionary.

      {
        process_name: string
        process_run: int
        state: string [WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST]
        (optional) start_time: seconds from epoch
        (optional) stop_time: seconds from epoch
      }
    """
    if len(history) == 0:
      return {}
    if run >= len(history):
      return {}
    else:
      process_run = history[run]
      run = run % len(history)
      d = dict(
        process_name = process_run.process,
        process_run = run,
        state = ProcessState._VALUES_TO_NAMES[process_run.state],
      )
      if process_run.start_time:
        d.update(start_time = process_run.start_time)
      if process_run.stop_time:
        d.update(stop_time = process_run.stop_time)
      return d

  @Lockable.sync
  def process(self, task_id, process, run=None):
    """
      Returns a process run, where the schema is given below:

      {
        process_name: string
        process_run: int
        used: { cpu: float, ram: int bytes, disk: int bytes }
        start_time: (time since epoch in millis (utc))
        stop_time: (time since epoch in millis (utc))
        state: string [WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST]
      }

      If run is None, return the latest run.
    """
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}
    if process not in state.processes:
      return {}
    history = state.processes[process]
    run = int(run) if run is not None else -1
    tup = self._get_process_tuple(history, run)
    if not tup:
      return {}
    if tup.get('state') == 'RUNNING':
      tup.update(used=self._get_process_resource_consumption(task_id, process))
    return tup

  @Lockable.sync
  def _processes(self, task_id):
    """
      Return
        {
          process1: { ... }
          process2: { ... }
          ...
          processN: { ... }
        }

      where processK is the latest run of processK and in the schema as
      defined by process().
    """

    if task_id not in self.all_tasks:
      return {}
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}

    processes = self._task_processes(task_id)
    d = dict()
    for process_type in processes:
      for process_name in processes[process_type]:
        d.update({ process_name: self.process(task_id, process_name) })
    return d

  @Lockable.sync
  def processes(self, task_ids):
    """
      Given a list of task_ids, returns a map of task_id => processes, where processes
      is defined by the schema in _processes.
    """
    if not isinstance(task_ids, (list, tuple)):
      return {}
    return dict((task_id, self._processes(task_id)) for task_id in task_ids)

  @Lockable.sync
  def get_run_number(self, runner_state, process, run=None):
    if runner_state is not None and runner_state.processes is not None:
      run = run if run is not None else -1
      if run < len(runner_state.processes[process]):
        if len(runner_state.processes[process]) > 0:
          return run % len(runner_state.processes[process])

  @Lockable.sync
  def logs(self, task_id, process, run = None):
    """
      Given a task_id and a process and (optional) run number, return a dict:
      {
        stderr: [dir, filename]
        stdout: [dir, filename]
      }

      If the run number is unspecified, uses the latest run.

      TODO(wickman)  Just return the filenames directly?
    """
    runner_state = self.raw_state(task_id)
    if runner_state is None or runner_state.header is None:
      return {}
    run = self.get_run_number(runner_state, process, run)
    if run is None:
      return {}
    log_path = self._pathspec.given(task_id=task_id, process=process, run=run,
                                    log_dir=runner_state.header.log_dir).getpath('process_logdir')
    return dict(
      stdout = [log_path, 'stdout'],
      stderr = [log_path, 'stderr']
    )

  @staticmethod
  def _sanitize_path(base_path, relpath):
    """
      Attempts to sanitize a path through path normalization, also making sure
      that the relative path is contained inside of base_path.
    """
    if relpath is None:
      relpath = "."
    normalized_base = os.path.realpath(base_path)
    normalized = os.path.realpath(os.path.join(base_path, relpath))
    if normalized.startswith(normalized_base):
      return (normalized_base, os.path.relpath(normalized, normalized_base))
    return (None, None)

  @Lockable.sync
  def valid_file(self, task_id, path):
    """
      Like valid_path, but also verify the given path is a file
    """
    chroot, path = self.valid_path(task_id, path)
    if chroot and path and os.path.isfile(os.path.join(chroot, path)):
      return chroot, path
    return None, None

  @Lockable.sync
  def valid_path(self, task_id, path):
    """
      Given a task_id and a path within that task_id's sandbox, verify:
        (1) it's actually in the sandbox and not outside
        (2) it's a valid, existing path
      Returns chroot and the pathname relative to that chroot.
    """
    runner_state = self.raw_state(task_id)
    if runner_state is None or runner_state.header is None:
      return None, None
    try:
      chroot = runner_state.header.sandbox
    except:
      return None, None
    chroot, path = self._sanitize_path(chroot, path)
    if chroot and path:
      return chroot, path
    return None, None

  @Lockable.sync
  def files(self, task_id, path=None):
    """
      Returns dictionary
      {
        task_id: task_id
        chroot: absolute directory on machine
        path: sanitized relative path w.r.t. chroot
        dirs: list of directories
        files: list of files
      }
    """
    # TODO(jon): DEPRECATED: most of the necessary logic is handled directly in the templates.
    # Also, global s/chroot/sandbox/?
    empty = dict(task_id=task_id, chroot=None, path=None, dirs=None, files=None)
    path = path if path is not None else '.'
    runner_state = self.raw_state(task_id)
    if runner_state is None:
      return empty
    try:
      chroot = runner_state.header.sandbox
    except:
      return empty
    if chroot is None:  # chroot-less job
      return empty
    chroot, path = self._sanitize_path(chroot, path)
    if (chroot is None or path is None
        or not os.path.isdir(os.path.join(chroot, path))):
      return empty
    names = os.listdir(os.path.join(chroot, path))
    dirs, files = [], []
    for name in names:
      if os.path.isdir(os.path.join(chroot, path, name)):
        dirs.append(name)
      else:
        files.append(name)
    return dict(
      task_id=task_id,
      chroot=chroot,
      path=path,
      dirs=dirs,
      files=files
    )
