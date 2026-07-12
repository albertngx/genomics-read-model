"""``cdc-load`` — the load-generator CLI.

Examples::

    cdc-load steady --rate 1000 --duration 300        # the acceptance target
    cdc-load burst --rate 200 --duration 120          # 10× spikes → visible consumer lag
    cdc-load reassignment-storm --rate 300 --seed 7   # §7 fan-out on hot runs
    cdc-load steady --rate 500 --ramp-to 2000         # ramp to find the knee

Every scenario is reproducible: the same ``--seed`` reproduces the op-kind stream, key
skew, and rate profile exactly (see :mod:`cdc_pipeline.tools.load.planner`).
"""

from __future__ import annotations

import asyncio

import typer

from cdc_pipeline.tools.load.client import LoadClient, resolve_api_url
from cdc_pipeline.tools.load.model import OpKind
from cdc_pipeline.tools.load.planner import WorkloadPlanner
from cdc_pipeline.tools.load.rates import Pacer, RampRate, RateProfile, total_expected
from cdc_pipeline.tools.load.scenarios import SCENARIOS, OpMix, get_scenario, parse_mix
from cdc_pipeline.tools.load.stats import Snapshot, StatsCollector, percentile


def _print_readout(snap: Snapshot, elapsed: float) -> None:
    typer.echo(
        f"[{elapsed:6.1f}s] {snap.achieved_rps:8.1f} req/s  "
        f"err {snap.error_rate * 100:5.2f}%  "
        f"p50 {snap.p50_ms:6.1f}ms  p99 {snap.p99_ms:6.1f}ms"
    )


def _print_summary(stats: StatsCollector, *, scenario: str, target_desc: str, seed: int) -> None:
    lat = stats.latencies
    line = "─" * 62
    out = typer.echo
    out("")
    out(line)
    out(f"  cdc-load summary — scenario={scenario}  seed={seed}")
    out(line)
    out(f"  target        : {target_desc}")
    out(f"  duration      : {stats.elapsed:.1f}s")
    out(f"  requests      : {stats.total:,}  ({stats.achieved_rps:,.1f} req/s achieved)")
    out(f"  errors        : {stats.errors:,}  ({stats.error_rate * 100:.3f}%)")
    if stats.skipped:
        out(f"  skipped       : {stats.skipped:,}  (drawn op had no eligible target)")
    out(f"  latency p50   : {percentile(lat, 50):.1f} ms")
    out(f"  latency p90   : {percentile(lat, 90):.1f} ms")
    out(f"  latency p99   : {percentile(lat, 99):.1f} ms")
    out(f"  latency max   : {percentile(lat, 100):.1f} ms")
    out("  by operation  :")
    for kind in OpKind:
        n = stats.by_kind.get(kind, 0)
        if n:
            errs = stats.errors_by_kind.get(kind, 0)
            out(f"      {kind.value:<11} {n:>9,}  ({errs:,} err)")
    if stats.error_reasons:
        out("  error reasons :")
        for reason, count in stats.error_reasons.most_common(5):
            out(f"      {reason:<11} {count:>9,}")
    out(line)


async def _execute(
    *,
    api_url: str,
    concurrency: int,
    pool_size: int,
    timeout: float,
    seed: int,
    scenario_name: str,
    mix: OpMix,
    profile: RateProfile,
    duration: float,
    stats: StatsCollector,
    insert_prefix: str,
) -> None:
    from cdc_pipeline.tools.load.runner import run_load  # local: keeps import time light

    async with LoadClient(api_url, concurrency=concurrency, timeout=timeout) as client:
        if not await client.health_ok():
            typer.echo(f"error: API at {api_url} is not healthy (GET /health failed)", err=True)
            raise typer.Exit(1)
        typer.echo(f"bootstrapping key pool from {api_url} (up to {pool_size:,} specimens)…")
        bootstrap = await client.bootstrap(pool_size)
        typer.echo(
            f"pool: {len(bootstrap.specimen_ids):,} specimens "
            f"({len(bootstrap.registered_ids):,} registered), {len(bootstrap.run_ids):,} runs"
        )
        scenario = get_scenario(scenario_name)
        planner = WorkloadPlanner.build(scenario, mix, bootstrap, seed, insert_prefix=insert_prefix)
        await run_load(
            planner=planner,
            client=client,
            pacer=Pacer(profile),
            duration=duration,
            concurrency=concurrency,
            stats=stats,
            on_readout=_print_readout,
        )


def _run_multiprocess(
    *,
    processes: int,
    api_url: str,
    scenario_name: str,
    mix_spec: str | None,
    rate: float,
    ramp_to: float | None,
    duration: float,
    concurrency: int,
    seed: int,
    pool_size: int,
    timeout: float,
    insert_prefix: str,
) -> StatsCollector:
    """Split the target rate across ``processes`` shards and run them in parallel."""
    from cdc_pipeline.tools.load.parallel import ShardSpec, run_parallel

    per_proc_rate = rate / processes
    per_proc_ramp = (ramp_to / processes) if ramp_to is not None else None
    specs = [
        ShardSpec(
            index=i,
            api_url=api_url,
            scenario_name=scenario_name,
            mix_spec=mix_spec,
            rate=per_proc_rate,
            ramp_to=per_proc_ramp,
            duration=duration,
            concurrency=concurrency,
            seed=seed + i,  # distinct-but-reproducible stream per shard
            pool_size=pool_size,
            timeout=timeout,
            insert_prefix=f"{insert_prefix}-p{i}",
        )
        for i in range(processes)
    ]
    return run_parallel(specs, duration)


def run(
    scenario: str = typer.Argument("steady", help=f"Scenario: {', '.join(SCENARIOS)}."),
    rate: float = typer.Option(1000.0, "--rate", "-r", help="Target requests/second."),
    duration: float = typer.Option(60.0, "--duration", "-d", help="Run length in seconds."),
    concurrency: int = typer.Option(
        64, "--concurrency", "-c", help="Async workers per process (in-flight cap)."
    ),
    processes: int = typer.Option(
        1, "--processes", "-p", help="Client processes across cores; --rate is split among them."
    ),
    seed: int = typer.Option(0, "--seed", "-s", help="RNG seed — makes the run reproducible."),
    ramp_to: float | None = typer.Option(
        None, "--ramp-to", help="Ramp the rate linearly from --rate to this value over --duration."
    ),
    mix: str | None = typer.Option(
        None, "--mix", help="Override the op mix, e.g. 'update=70,insert=15,dispose=10,delete=5'."
    ),
    pool_size: int = typer.Option(
        3000, "--pool-size", help="Max existing specimens to load into the Zipf key universe."
    ),
    api_url: str | None = typer.Option(
        None, "--api-url", help="API base URL (default: $API_URL or http://localhost:$API_PORT)."
    ),
    timeout: float = typer.Option(10.0, "--timeout", help="Per-request timeout in seconds."),
    run_tag: str | None = typer.Option(
        None, "--run-tag", help="Barcode prefix for inserted specimens (default: random)."
    ),
) -> None:
    """Drive write load through the tracking API and print a live readout + summary."""
    import uuid

    scenario_obj = get_scenario(scenario)
    op_mix = parse_mix(mix) if mix else scenario_obj.mix
    profile: RateProfile = (
        RampRate(rate, ramp_to, duration)
        if ramp_to is not None
        else scenario_obj.rate_factory(rate, duration)
    )
    # Barcodes must stay globally unique across repeated runs, so the prefix carries a
    # random token by default. The *count and order* of inserts stays seed-reproducible.
    insert_prefix = run_tag or f"LOAD-{uuid.uuid4().hex[:8]}"
    resolved_url = resolve_api_url(api_url)

    if processes < 1:
        raise typer.BadParameter("--processes must be >= 1")

    proc_note = f" · {processes} processes" if processes > 1 else ""
    typer.echo(
        f"cdc-load: scenario={scenario} · {profile.describe()} · "
        f"{concurrency} workers/proc{proc_note} · {duration:g}s · seed={seed}"
    )
    mix_pct = ", ".join(f"{k.value}={v:.0f}%" for k, v in op_mix.as_percentages().items())
    typer.echo(f"  mix: {mix_pct}")
    typer.echo(f"  ≈{total_expected(profile, duration):,} operations planned")
    typer.echo(f"  target API: {resolved_url}")

    interrupted = False
    try:
        if processes > 1:
            stats = _run_multiprocess(
                processes=processes,
                api_url=resolved_url,
                scenario_name=scenario,
                mix_spec=mix,
                rate=rate,
                ramp_to=ramp_to,
                duration=duration,
                concurrency=concurrency,
                seed=seed,
                pool_size=pool_size,
                timeout=timeout,
                insert_prefix=insert_prefix,
            )
        else:
            stats = StatsCollector()
            asyncio.run(
                _execute(
                    api_url=resolved_url,
                    concurrency=concurrency,
                    pool_size=pool_size,
                    timeout=timeout,
                    seed=seed,
                    scenario_name=scenario,
                    mix=op_mix,
                    profile=profile,
                    duration=duration,
                    stats=stats,
                    insert_prefix=insert_prefix,
                )
            )
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        interrupted = True
        typer.echo("\ninterrupted — reporting partial results…")
        stats = locals().get("stats") or StatsCollector()

    _print_summary(stats, scenario=scenario, target_desc=profile.describe(), seed=seed)

    # Exit non-zero if the acceptance bar (<0.1% errors) was missed, so `make load`/CI can gate.
    if stats.total and stats.error_rate >= 0.001:
        typer.echo(f"error rate {stats.error_rate * 100:.3f}% exceeds the 0.1% budget", err=True)
        raise typer.Exit(1)
    if interrupted:
        raise typer.Exit(130)


def main() -> None:
    """Console-script entrypoint (``cdc-load``). A single-command app invoked directly."""
    typer.run(run)


if __name__ == "__main__":  # pragma: no cover
    main()
