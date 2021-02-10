![Aurora Logo](docs/images/aurora_logo.png)

![Build Status](https://github.com/aurora-scheduler/aurora/workflows/CI/badge.svg?branch=master)

[Aurora Scheduler](https://aurora-scheduler.github.io/) lets you use an [Apache Mesos](http://mesos.apache.org)
cluster as a private cloud. It supports running long-running services, cron jobs, and ad-hoc jobs.
Aurora aims to make it extremely quick and easy to take a built application and run it on machines
in a cluster, with an emphasis on reliability. It provides basic operations to manage services
running in a cluster, such as rolling upgrades.

To very concisely describe Aurora, it is like a distributed monit or distributed supervisord that
you can instruct to do things like _run 100 of these, somewhere, forever_.

### What this project is and what it is not
Aurora Scheduler is a mainteannce fork of Apache Aurora after the original project
entered the [Apache Attic](https://lists.apache.org/thread.html/reee926fceea75b7cc25110abb9deb3c41921c1585157a7f45c814419%40%3Cdev.aurora.apache.org%3E).
The goal of this fork is to continue maintenance and  quality of life updates to the **scheduler codebase**
but we don't anticipate any new large features being landed.

**Please note that this also means the [original Python2 client](https://github.com/aurora-scheduler/client) is unmaintained.**

**Changes made to the scheduler will always strive to be compatible with existing tools but compatibility is _not_ guaranteed.**

## Features

Aurora is built for users _and_ operators.

* User-facing Features:
  - Management of [long-running services](docs/features/services.md)
  - [Cron jobs](docs/features/cron-jobs.md)
  - [Resource quotas](docs/features/multitenancy.md): provide guaranteed resources for specific
    applications
  - [Rolling job updates](docs/features/job-updates.md), with automatic rollback
  - [Multi-user support](docs/features/multitenancy.md)
  - Sophisticated [DSL](docs/reference/configuration-tutorial.md): supports templating, allowing you to
    establish common patterns and avoid redundant configurations
  - [Dedicated machines](docs/features/constraints.md#dedicated-attribute):
    for things like stateful services that must always run on the same machines
  - [Service registration](docs/features/service-discovery.md): announce services in
    [ZooKeeper](http://zookeeper.apache.org/) for discovery by [various clients](docs/additional-resources/tools.md)
  - [Scheduling constraints](docs/features/constraints.md)
    to run on specific machines, or to mitigate impact of issues like machine and rack failure

* Under the hood, to help you rest easy:
  - [Preemption](docs/features/multitenancy.md): important services can 'steal' resources when they need it
  - High-availability: resists machine failures and disk failures
  - Scalable: proven to work in data center-sized clusters, with hundreds of users and thousands of
    jobs
  - Instrumented: a wealth of information makes it easy to [monitor](docs/operations/monitoring.md)
    and debug

### When and when not to use Aurora
**Diclaimer:** _If you are not already using Aurora, it is strongly suggested you look into other projects
in this space as the Mesos ecosystem (including our project) is in maintenance mode._

_We are extremely proud of what has been achieved by this project but the reality is there are other
projects with a much more active community behind them where users will be able to enjoy better support._

Aurora can take over for most uses of software like monit and chef.  Aurora can manage applications,
while these tools are still useful to manage Aurora and Mesos themselves.

However, if you have very specific scheduling requirements, or are building a system that looks like a
scheduler itself, you may want to explore developing your own
[framework](http://mesos.apache.org/documentation/latest/app-framework-development-guide).


## Getting Help
If you have questions that aren't answered in our [documentation](https://aurora-scheduler.github.io/documentation/latest/),
you can reach out to the community via Matrix on [#aurora-scheduler](https://matrix.to/#/#aurora-scheduler:matrix.org).

You can also file bugs/issues in our [Github](https://github.com/aurora-scheduler/aurora/issues) repo.


## License
Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
