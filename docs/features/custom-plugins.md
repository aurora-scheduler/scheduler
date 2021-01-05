Custom Plugins
==============

OfferSet
--------
OfferSet holds all offers for the scheduler. OfferSet is customizable via Interface `org.apache.aurora.scheduler.offers.OfferSet`.
This interface is injectable via the '-offer_set_module' flag.
By default, Aurora does not sort or filter OfferSet.
We can customize the offerset that changes the behavior of the scheduler.
For example, we can spread out the load by sorting the offers.

One of the limitations of this approach is that you need to develop your own OfferSet implementation and compile it with Apache Aurora.
To make OfferSet plugin more flexible, we proposed HTTP OfferSet that allows us to sorts or filters the offers externally via http.

* HTTP OfferSet
We added HTTP OfferSet `io.github.aurora.scheduler.offers.HttpOfferSetModule` that allows Aurora to talk to an external REST API server.

How to configure HTTP OfferSet?
- offer_set_module=io.github.aurora.scheduler.offers.HttpOfferSetModule 
- http_offer_set_timeout_ms is http timeout in milliseconds.
- http_offer_set_max_retries is the number of retries if the module fails to connects to the external REST API server.
If it exceeds the number of retries, Aurora uses the native offerset implementation.
- http_offer_set_endpoint is the REST API endpoint, e.g. http://127.0.0.1:9090/v1/offerset.

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
