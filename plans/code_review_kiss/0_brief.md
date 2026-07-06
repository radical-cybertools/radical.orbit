# ORBIT — code review brief (KISS / academic readability)

You are reviewing the ORBIT codebase. This document is your ONLY briefing.
**Do not read any file under `plans/`, `ROADMAP*`, `docs/`, or any design /
discussion / changelog document, and do not infer intent from git history.**
Judge the code as it stands against the requirements below. The burden is on
the code to justify its complexity to you — where it does not, say so.

## What ORBIT is (architecturally)

ORBIT connects external applications with HPC resources. It is a **star**: one
active **broker** hub routes messages between **participants**. Each participant
dials the hub over a single outbound connection and may **serve** capabilities,
**consume** other participants' capabilities, or both. Capabilities are provided
by **plugins**. A broker-hosted **gateway** exposes an HTTP/SSE surface so
non-participant clients (a browser UI, other-language clients) can interact too.

## Use cases (NON-EXHAUSTIVE — many more are expected)

These are illustrative, not a fixed set. **Do not optimize your reasoning around
these specific scenarios or their domain semantics** — new use cases will keep
arriving, and each one's *meaning* (how to submit a SLURM job, how to run a
task, how to move a file) lives inside a **plugin**, not in the core. What
matters is the *architectural shape* these imply, which is stable.

1. A workflow manager running inside an HPC allocation is simultaneously a
   server (executing tasks others send it) and a consumer (delegating sub-work
   to participants on other resources).
2. An application submits jobs / tasks to a remote HPC resource through the hub.
3. A participant on a firewalled HPC node reaches the hub with outbound-only
   connectivity.
4. Long-running work streams status/notifications back to interested consumers
   and to browser/HTTP clients.
5. Non-participant HTTP clients interact through a compatibility surface.
6. Bulk data moves between resources out-of-band (not through the hub).
7. Work and sessions survive transient disconnects/restarts; long-lived
   resources are reclaimed only when their owner truly goes away.

## Architectural requirements (the stable core the code must serve — SIMPLY)

- One hub routes between participants; participants dial in over a single
  outbound connection and may serve and/or consume.
- A uniform message contract lets any participant invoke any plugin on any other
  participant and push events, in either direction.
- Failure detection reflects host/network health and is decoupled from plugin
  behavior (a slow or blocking plugin must not look like a dead host).
- **Plugins are the only place domain semantics live.** The core (hub, runtime,
  message contract, plugin framework, gateway) must stay small, general, and
  semantics-agnostic. New use cases are meant to arrive as *new plugins*, not as
  changes to the core — so the core's simplicity and the plugin interface's
  clarity are the most important things in this codebase.
- A compatibility ingress (HTTP/SSE/UI) serves non-participants.
- Access is gated at ingress.

## Your mandate: challenge complexity, favor readability (KISS)

This is an **academic project**. It is paramount that students can **read,
understand, run, and debug** the code easily. Cleverness and undue complexity
are actively harmful here. Your job is to **challenge assumptions** and hunt for
simplicity, not to admire the design.

For every module in your assigned scope, ask:

- Could a student unfamiliar with the code follow the control flow on a first
  read? Where does it break down?
- Is each abstraction, layer, indirection, thread, queue, callback, or
  metaclass **earning its keep**, or could the same behavior be expressed more
  directly with plain functions / straight-line code / the standard library?
- Is there **speculative generality** — flexibility, configurability, hooks, or
  parameters that no current requirement needs?
- Is there **duplication** — near-identical code that drifted into variants, or
  parallel structures that could be one?
- Is the **naming** honest and obvious? Do names match what the code does?
- Is the **error handling** proportionate, or ceremonial / swallowing?
- Is concurrency (threads, event loops, async, locks) used where it is genuinely
  required, or where a simpler sequential form would do?

**Essential vs accidental complexity.** Some complexity is *required* by a real
architectural requirement above (e.g. outbound-only firewall traversal;
keeping failure-detection responsive while a plugin blocks; not corrupting
shared state under concurrency). Distinguish this from *accidental* complexity
that is incidental to the current implementation. When complexity appears
essential, say which requirement forces it — and still ask whether a simpler
implementation of that same requirement exists. When you cannot tie complexity
to a requirement, treat it as unjustified and challenge it.

Lean toward challenging. A finding that turns out to be justified costs a short
discussion; unquestioned complexity costs every future student.

## How to report (write to the output file you are given; change NO code)

Produce a markdown findings list. For each finding:

- **Title** — one line naming the assumption or complexity.
- **Location** — `file:line` (or symbol) and the module.
- **What & why it hurts** — what the code does, and why it burdens a student
  reading/running/debugging it.
- **Simpler alternative** — a concrete, specific suggestion (not "consider
  simplifying"). Sketch the shape of the simpler form.
- **Essential?** — is the underlying behavior required by an architectural
  requirement (name it) or not? If essential, does a simpler implementation of
  that requirement exist?
- **Severity** — high / medium / low (student-comprehension impact).
- **Effort** — rough (small / medium / large).

Start the file with a 3–5 sentence overall impression of your scope's
simplicity, then the findings ordered most-severe first. Be concrete and cite
line numbers. Do not propose changes you have not located in the code. Do not
edit any source file.
