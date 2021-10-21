Custom Plugins
==============

OfferSet
--------
OfferSet holds all offers for the scheduler. OfferSet is customizable via Interface `org.apache.aurora.scheduler.offers.OfferSet`.
This interface is injectable via the '-offer_set_module' flag.
By default, Aurora does not sort or filter OfferSet.
We can customize the offerset that changes the behavior of the scheduler.
For example, we can spread out the load by sorting the offers.

#### HTTP OfferSet
One of the limitations of this approach is that you need to develop your own OfferSet implementation and compile it with Apache Aurora.
To make OfferSet plugin more flexible, we proposed HTTP OfferSet that allows us to sorts or filters the offers externally via http.

We added HTTP OfferSet `io.github.aurora.scheduler.offers.HttpOfferSetModule` that allows Aurora to talk to an external REST API server.

How to configure HTTP OfferSet?
- offer_set_module=io.github.aurora.scheduler.offers.HttpOfferSetModule 
- http_offer_set_timeout is http timeout value. `100ms` is the default value.
- http_offer_set_max_retries is the number of retries if the module fails to connects to the external REST API server.
`10` is the default value.
If it exceeds the number of retries, HTTP OfferSet will not reach the external endpoint anymore.
- http_offer_set_endpoint is the REST API endpoint, e.g. http://127.0.0.1:9090/v1/offerset.
- http_offer_set_max_starting_tasks_per_slave is the maximum starting tasks per slave.
If a slave has more than this number of starting tasks, it will be put at the end of offer list. 
This feature is useful when a node is vulnerable to a certain number of tasks starting at the same time.
A task often demands a lot of resources during STARTING (e.g., pulling docker images and frequent healchecks).
Too many of them starting at the same time may overload the nodes.
It is disabled by default.
If you want to use this feature, please set this parameter a positive integer number.

- http_offer_set_task_fetch_interval determine how often HTTP OfferSet fetches the starting tasks from the `task_store`.
By default, it is `1secs`. 

How to implement the external REST API server?
The REST API needs to handle the request in the following format:
```
{
    "jobKey":"test-dev-job-1",
    "request":{"cpu":1,"memory":1,"disk":0}, 
    "hosts": [
        {
            "name": "agent-1",
            "offer": {"cpu": 1.0,"memory": 1024.0, "disk": 1048576.0}
        },
        {
            "name": "agent-2",
            "offer": {"cpu": 3.0,"memory": 2048.0, "disk": 1048576.0}
        } ,
        {
            "name": "agent-3",
            "offer": {"cpu": 2.0,"memory": 2048.0, "disk": 1048576.0}
        } 
    ]
}
```
While 1 cpu is equivalent to 1 vcpu, memory and disk are in MB.

The server returns the response in the following format.
```
{
    "errors":"",
    "hosts": [
        "agent-2",
        "agent-1",
        "agent-3",
    ]
}
```
In the above example, the external REST API sorts the offers based on the number of available vcpus.

How to monitor HTTP OfferSet?
We can monitor this plugin by looking at the endpoint `/vars`. The following metrics are available when HTTP OfferSet is enabled:
- `http_offer_set_avg_latency_ms`: The average latency per scheduling cycle in milliseconds.
- `http_offer_set_median_latency_ms`: The median latency per scheduling cycle in milliseconds.
- `http_offer_set_worst_latency_ms`: The worst latency per scheduling cycle in milliseconds.
- `http_offer_set_failure_count`: The number of scheduling failures.
- `http_offer_set_max_diff`: The number of different offers between the original `OfferSet` and the received one.

HTTP OfferSet resets the above metrics every `sla_stat_refresh_interval`.

TaskAssigner
--------
TaskAssigner is the plugin module that allows us to match a group of tasks to a set of offers.
By default, `org.apache.aurora.scheduler.scheduling.TaskAssignerImplModule` does matching in a FIFO manner.
We can take advantage of TaskAssigner when there is additional requirement.

#### Probabilistic priority queueing
Even though there is `priority` in `TaskConfig`, aurora-scheduler does not support priority queueing. 
`priority` is mainly used for `preemption` which applied to the tasks after `TASK_ASSIGNED`.
When jobs (or services) are pending for scheduling, we can prioritize the `jobs` with higher priorities. 
In this approach, we do not offer hard priority queueing that strictly blocks lower priority tasks from being scheduled.
Instead, we offer a higher chance of being scheduled to jobs with higher priority and a lower chance to jobs with a lower priority.
We will refer to this approach as `probabilistic priority queueing`.
To enable `probabilistic priority queueing`, you need to set the following parameters
- `task_assigner_modules=io.github.aurora.scheduler.scheduling.ProbabilisticPriorityAssignerModule`
- `probabilistic_priority_assigner_exponent=[non-negative double like 1.0]`

`io.github.aurora.scheduler.scheduling.ProbabilisticPriorityAssignerModule` is the plugin module while 
`probabilistic_priority_assigner_exponent` is its control parameter.
The non-negative chance of scheduling a task with `priority` is computed by `(priority + 1)^probabilistic_priority_assigner_exponent`. 
For example, there are `N` pending jobs with 2 priorities `{0, 1}`. How does the `chance` impact on scheduling these jobs?  

- If `probabilistic_priority_assigner_exponent=1.0`, the chance of `0` is `1` while the chance of `1` is `2`. 
If the scheduler has to schedule 900 jobs first from the queue, it likely schedules 600 jobs with `priority=1` and 300 jobs with `priority=0`. 
- If `probabilistic_priority_assigner_exponent=3.0`, the chance of `0` is `1` while the chance of `1` is `8`.
If the scheduler has to schedule 900 jobs first from the queue, it likely schedules 800 jobs with `priority=1` and 100 jobs with `priority=0`.
- If `probabilistic_priority_assigner_exponent=0.0`, the chance of `0` is `1` while the chance of `1` is `1`. 
If the scheduler has to schedule 900 jobs first from the queue, it likely schedules 450 jobs with `priority=1` and 450 jobs with `priority=0`.
In this case, `probabilistic priority queueing` behaves like the default `TaskAssigner`.
