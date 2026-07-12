<script lang="ts">
  import ClusterGraph from '$lib/ClusterGraph.svelte';
  import type { Snapshot } from '$lib/types';
  import { isHealthy } from '$lib/types';
  import { onMount } from 'svelte';

  let snap = $state<Snapshot | null>(null);
  let selectedId = $state<number | null>(null);
  let toast = $state('');
  let busy = $state(false);
  let live = $state(false);

  onMount(() => {
    const es = new EventSource('/api/stream');
    es.onmessage = (ev) => {
      try {
        snap = JSON.parse(ev.data) as Snapshot;
        live = true;
        if (selectedId == null && snap.nodes.length) {
          selectedId = snap.nodes[0].id;
        }
      } catch {
        /* ignore */
      }
    };
    es.onerror = () => {
      live = false;
    };
    return () => es.close();
  });

  let selected = $derived(snap?.nodes.find((n) => n.id === selectedId) ?? null);

  async function act(action: 'kill' | 'restart' | 'partition') {
    if (!selectedId || busy) return;
    busy = true;
    toast = `${action} node ${selectedId}…`;
    try {
      const res = await fetch(`/api/nodes/${selectedId}/${action}`, { method: 'POST' });
      const text = await res.text();
      toast = res.ok ? `ok — ${action} node ${selectedId}` : text;
    } catch (err) {
      toast = String(err);
    } finally {
      busy = false;
    }
  }

  async function resetAll() {
    busy = true;
    toast = 'reset all…';
    try {
      const res = await fetch('/api/reset', { method: 'POST' });
      toast = res.ok ? 'ok — reset all' : await res.text();
    } catch (err) {
      toast = String(err);
    } finally {
      busy = false;
    }
  }
</script>

<svelte:head>
  <title>Kill My Cluster</title>
</svelte:head>

<main>
  <header>
    <div>
      <h1>Kill My Cluster</h1>
      <p class="sub">
        Live Raft mesh — kill or partition a node and watch auto-heal reconnect the graph.
        <span class="dot" class:on={live}></span>
        {live ? 'live' : 'reconnecting…'}
      </p>
    </div>
    <a class="grafana" href="http://localhost:3000" target="_blank" rel="noreferrer">Grafana ↗</a>
  </header>

  {#if snap && !snap.quorum}
    <div class="banner">
      <strong>Quorum lost.</strong> A majority of nodes must be healthy — the cluster chooses safety
      over lying.
    </div>
  {/if}

  <div class="layout">
    <section class="stage">
      {#if snap}
        <ClusterGraph
          nodes={snap.nodes}
          {selectedId}
          onselect={(id) => {
            selectedId = id;
          }}
        />
      {:else}
        <p class="loading">Connecting to control plane…</p>
      {/if}
    </section>

    <aside class="panel">
      <div class="stats">
        {#if snap}
          <div><span>Alive</span><strong>{snap.alive}/{snap.total}</strong></div>
          <div><span>Quorum</span><strong class:bad={!snap.quorum}>{snap.quorum ? 'yes' : 'NO'}</strong></div>
          <div><span>Heal</span><strong>{snap.healAfterMs}ms</strong></div>
        {/if}
      </div>

      {#if selected}
        <h2>Node {selected.id}</h2>
        <p class="meta">{selected.container}</p>
        <p class="meta">
          {#if isHealthy(selected)}
            healthy on the mesh
          {:else if !selected.running}
            offline
          {:else}
            partitioned (off network)
          {/if}
          {#if selected.healDueMs > 0}
            · healing in {(selected.healDueMs / 1000).toFixed(1)}s
          {/if}
        </p>
        <div class="actions">
          <button class="kill" disabled={busy || !selected.running || selected.partitioned} onclick={() => act('kill')}
            >Kill</button
          >
          <button
            class="partition"
            disabled={busy || !selected.running || selected.partitioned}
            onclick={() => act('partition')}>Partition</button
          >
          <button
            class="restart"
            disabled={busy || (selected.running && !selected.partitioned)}
            onclick={() => act('restart')}>Restart</button
          >
        </div>
      {:else}
        <p class="meta">Select a node on the graph.</p>
      {/if}

      <button class="reset" disabled={busy} onclick={resetAll}>Reset all</button>
      <p class="toast">{toast}</p>

      <h3>Feed</h3>
      <div class="feed">
        {#if snap}
          {#each [...snap.events].reverse() as ev}
            <div class="ev">
              <strong>{ev.kind}</strong>
              {new Date(ev.time).toLocaleTimeString()} — {ev.message}
            </div>
          {/each}
        {/if}
      </div>
    </aside>
  </div>
</main>

<style>
  :global(html, body) {
    margin: 0;
    min-height: 100%;
    background: radial-gradient(ellipse at top, #152030 0%, #0b0f14 55%);
    color: #e8eef4;
    font-family: 'IBM Plex Sans', ui-sans-serif, system-ui, sans-serif;
  }

  main {
    max-width: 1100px;
    margin: 0 auto;
    padding: 1.5rem 1.25rem 2.5rem;
  }

  header {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: flex-start;
    margin-bottom: 1rem;
  }

  h1 {
    margin: 0;
    font-size: 1.5rem;
    letter-spacing: -0.02em;
  }

  .sub {
    margin: 0.35rem 0 0;
    color: #7a8fa3;
    font-size: 0.92rem;
  }

  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #ff5c5c;
    margin: 0 0.25rem 0 0.5rem;
    vertical-align: middle;
  }

  .dot.on {
    background: #3dd68c;
  }

  .grafana {
    color: #f0c14b;
    text-decoration: none;
    font-size: 0.9rem;
    white-space: nowrap;
  }

  .banner {
    margin-bottom: 1rem;
    padding: 0.75rem 1rem;
    border-radius: 8px;
    background: rgba(255, 92, 92, 0.12);
    border: 1px solid rgba(255, 92, 92, 0.35);
    color: #ffb4b4;
  }

  .layout {
    display: grid;
    grid-template-columns: 1.4fr 0.9fr;
    gap: 1.25rem;
    align-items: start;
  }

  @media (max-width: 860px) {
    .layout {
      grid-template-columns: 1fr;
    }
  }

  .stage {
    background: #121a24;
    border: 1px solid #1e2d3d;
    border-radius: 14px;
    padding: 0.75rem;
    min-height: 360px;
  }

  .loading {
    color: #7a8fa3;
    text-align: center;
    padding: 4rem 1rem;
  }

  .panel {
    background: #121a24;
    border: 1px solid #1e2d3d;
    border-radius: 14px;
    padding: 1rem 1.1rem;
  }

  .stats {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0.5rem;
    margin-bottom: 1rem;
    padding-bottom: 0.85rem;
    border-bottom: 1px solid #1e2d3d;
  }

  .stats span {
    display: block;
    color: #7a8fa3;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .stats strong {
    font-size: 1.05rem;
  }

  .stats strong.bad {
    color: #ff5c5c;
  }

  h2 {
    margin: 0;
    font-size: 1.1rem;
  }

  h3 {
    margin: 1rem 0 0.4rem;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #7a8fa3;
  }

  .meta {
    color: #7a8fa3;
    font-size: 0.85rem;
    margin: 0.25rem 0 0.85rem;
  }

  .actions {
    display: flex;
    gap: 0.4rem;
    flex-wrap: wrap;
  }

  button {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.45rem 0.65rem;
    border-radius: 7px;
    border: 1px solid #1e2d3d;
    background: #1a2634;
    color: #e8eef4;
    cursor: pointer;
  }

  button:disabled {
    opacity: 0.35;
    cursor: not-allowed;
  }

  .kill {
    border-color: rgba(255, 92, 92, 0.45);
    color: #ffb4b4;
  }

  .partition {
    border-color: rgba(183, 148, 246, 0.45);
    color: #b794f6;
  }

  .restart {
    border-color: rgba(61, 214, 140, 0.4);
    color: #3dd68c;
  }

  .reset {
    width: 100%;
    margin-top: 0.75rem;
  }

  .toast {
    min-height: 1.2em;
    color: #7a8fa3;
    font-size: 0.82rem;
    margin: 0.6rem 0 0;
  }

  .feed {
    max-height: 220px;
    overflow-y: auto;
    font-size: 0.78rem;
    color: #7a8fa3;
  }

  .ev {
    padding: 0.3rem 0;
    border-bottom: 1px solid rgba(30, 45, 61, 0.55);
  }

  .ev strong {
    color: #e8eef4;
  }
</style>
