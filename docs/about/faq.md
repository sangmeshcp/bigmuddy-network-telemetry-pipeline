# FAQ

**Does it replace SNMP?**
For counters and state that devices expose over gNMI or MDT, yes: with
higher resolution and lower device CPU. Many teams run both while migrating.
Traps and syslog are out of scope.

**Which vendors are supported?**
Any device with a gNMI server: Cisco IOS XR / XE / NX-OS, Juniper
Junos/Evolved, Arista EOS, Nokia SR OS / SR Linux, SONiC, and others.
Cisco devices can also use MDT dial-out. Conformance is tested against the
OpenConfig reference implementation of gNMI, not against one vendor.

**OpenConfig or native models?**
Prefer OpenConfig paths for a multi-vendor fleet (one dashboard for every
vendor). Use native paths for data OpenConfig doesn't model. Both work the
same way in the pipeline.

**Why not Telegraf or gNMIc?**
Both are good tools. This project focuses on network telemetry only, keeps
the 2016 pipeline's Cisco MDT dial-out compatibility, and is small enough to
read in an afternoon. If you already run Telegraf for everything, its
`gnmi` and `cisco_telemetry_mdt` inputs may suit you better.

**Is data lost when an output is down?**
Each output buffers up to `queue_size` metrics and retries transient errors.
Beyond that, data for *that output* is dropped and counted, by design, so
other outputs keep working. For guaranteed delivery, write to Kafka and
consume from there, since Kafka's own retention covers long outages.

**Why are counters exported as gauges?**
Telemetry doesn't say which leaves are counters. Use `rate()` at query time;
see [Data model](../concepts/data-model.md#counters-vs-gauges).

**How many devices can one instance handle?**
It depends on metrics per second rather than device count. See
[Deployment → Sizing](../operations/deployment.md#sizing-guidance) and watch
`pipeline_router_queue_batches`.

**Can I still use my 2016 `pipeline.conf`?**
No, but the [migration table](modernization.md#4-migrating-from-pipelineconf)
maps every section to its new equivalent.
