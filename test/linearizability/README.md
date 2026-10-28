# Linearizability (Porcupine) runs

`make chaos` starts a 3-node in-process cluster, runs concurrent clients
issuing put / get / compare-and-swap through random nodes while a nemesis
kills, partitions and freezes nodes, then checks the recorded history with
[Porcupine](https://github.com/anishathalye/porcupine).

Results are appended to `docs/chaos/RESULTS.md`; a visualization of every run
is written next to it. Re-run a specific history with `CHAOS_SEED=<seed>`.
