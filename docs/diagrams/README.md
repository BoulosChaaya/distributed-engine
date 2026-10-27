# System diagrams

Six standard views of the engine. Each answers one question, and each is written in
Mermaid, so GitHub renders it here and a change to it is reviewed like code.

| View | Question it answers | File |
|---|---|---|
| C4 model | What are the parts, and who talks to whom? | [01-c4.md](01-c4.md) |
| Sequence diagrams | What happens, in order, for each important flow? | [02-sequences.md](02-sequences.md) |
| State machines | What can a task, an outbox event, a workflow, a tenant become? | [03-state-machines.md](03-state-machines.md) |
| Data model | What is stored, and how do the tables connect? | [04-data-model.md](04-data-model.md) |
| Tracing | What does one request look like at runtime, and how do I see the live map? | [05-tracing.md](05-tracing.md) |
| Event storming | Which commands cause which events, and which rules react to them? | [06-event-storming.md](06-event-storming.md) |

The one rule behind all six: **PostgreSQL decides what is true.** Redis, the
workers and the API can each die; the system re-derives its state from PostgreSQL.

These diagrams describe v1.0.2. Names (tables, statuses, span names, events)
are the ones in the code; if a diagram and the code disagree, the code wins
and the diagram is the bug.
