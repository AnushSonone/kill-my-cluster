<script lang="ts">
  import ClusterGraph from '$lib/ClusterGraph.svelte';
  import type { Snapshot } from '$lib/types';
  import { GRAFANA_EMBED } from '$lib/types';
  import { preferCluster3D } from '$lib/webgl';
  import { onMount, type Component } from 'svelte';

  let snap = $state<Snapshot | null>(null);
  let selectedId = $state<number | null>(null);
  let toast = $state('');
  let busy = $state(false);
  let live = $state(false);
  let grafanaOpen = $state(false);
  /** null until client decides; then ClusterScene or stays null for SVG. */
  let ClusterScene = $state<Component<{
    nodes: Snapshot['nodes'];
    selectedId: number | null;
    onselect: (id: number) => void;
  }> | null>(null);
  let use3d = $state(false);
  /** Kill ids waiting for Docker to report exited — stops SSE from undoing optimistic UI. */
  const pendingKills = new Map<number, number>();

  let healLabel = $derived(snap ? `${(snap.healAfterMs / 1000).toFixed(0)}s` : '…');
  let selected = $derived(snap?.nodes.find((n) => n.id === selectedId) ?? null);

  let canKill = $derived(
    !!selectedId &&
      !busy &&
      !!snap?.quorum &&
      !!selected &&
      selected.running &&
      !selected.partitioned
  );

  function applySnapshot(raw: Snapshot) {
    const nodes = raw.nodes.map((n) => {
      const deadline = pendingKills.get(n.id);
      if (deadline == null) return n;
      if (!n.running) {
        pendingKills.delete(n.id);
        return n;
      }
      // Still "running" in SSE — keep showing the kill until Docker catches up.
      return {
        ...n,
        running: false,
        status: 'exited',
        isLeader: false,
        healDueMs: Math.max(n.healDueMs, deadline - Date.now()),
        healKind: n.healKind || 'start'
      };
    });
    const alive = nodes.filter((n) => n.running && !n.partitioned).length;
    snap = { ...raw, nodes, alive, quorum: alive > raw.total / 2 };
  }

  onMount(() => {
    let cancelled = false;
    if (preferCluster3D()) {
      import('$lib/ClusterScene.svelte')
        .then((mod) => {
          if (cancelled) return;
          ClusterScene = mod.default;
          use3d = true;
        })
        .catch(() => {
          use3d = false;
        });
    }

    const es = new EventSource('/api/stream');
    es.onmessage = (ev) => {
      try {
        applySnapshot(JSON.parse(ev.data) as Snapshot);
        live = true;
        if (selectedId == null && snap?.nodes.length) {
          const leader = snap.nodes.find((n) => n.isLeader);
          selectedId = leader?.id ?? snap.nodes[0].id;
        }
      } catch {
        /* ignore */
      }
    };
    es.onerror = () => {
      live = false;
    };
    return () => {
      cancelled = true;
      es.close();
    };
  });

  async function killSelected() {
    if (!selectedId || !canKill || !snap) return;
    const id = selectedId;
    busy = true;
    toast = `Killing Machine ${id}…`;
    const healMs = snap.healAfterMs;
    pendingKills.set(id, Date.now() + healMs);
    applySnapshot({
      ...snap,
      nodes: snap.nodes.map((n) =>
        n.id === id
          ? {
              ...n,
              running: false,
              status: 'exited',
              isLeader: false,
              role: undefined,
              healDueMs: healMs,
              healKind: 'start'
            }
          : n
      )
    });
    try {
      const res = await fetch(`/api/nodes/${id}/kill`, { method: 'POST' });
      const text = await res.text();
      toast = res.ok ? `Machine ${id} down · heals in ${healLabel}` : text;
      if (!res.ok) pendingKills.delete(id);
    } catch (err) {
      toast = String(err);
      pendingKills.delete(id);
    } finally {
      busy = false;
    }
  }
</script>

<svelte:head>
  <title>Kill My Cluster</title>
</svelte:head>

<main class:immersive={use3d}>
  <div class="stage" class:fullscreen={use3d}>
    {#if snap}
      {#if use3d && ClusterScene}
        <ClusterScene
          nodes={snap.nodes}
          {selectedId}
          onselect={(id) => {
            selectedId = id;
          }}
        />
      {:else}
        <ClusterGraph
          nodes={snap.nodes}
          {selectedId}
          onselect={(id) => {
            selectedId = id;
          }}
        />
      {/if}
    {:else}
      <p class="loading">Connecting…</p>
    {/if}
  </div>

  <header class="hud-top">
    <h1>Kill My Cluster</h1>
    <span class="live" class:on={live}>{live ? 'live' : '…'}</span>
    {#if use3d}
      <span class="hint">scroll to zoom · drag to orbit</span>
    {/if}
  </header>

  {#if snap && !snap.quorum}
    <div class="banner">Quorum lost — waiting for majority.</div>
  {/if}

  <aside class="hud-panel">
    <div class="stats">
      {#if snap}
        <div><span>Alive</span><strong>{snap.alive}/{snap.total}</strong></div>
        <div><span>Leader</span><strong>{snap.leaderId ? `M${snap.leaderId}` : '—'}</strong></div>
        <div><span>Term</span><strong>{snap.term ?? '—'}</strong></div>
        <div><span>Heal</span><strong>{healLabel}</strong></div>
      {/if}
    </div>

    <button class="kill" disabled={!canKill} onclick={killSelected}>
      {selected ? `Kill Machine ${selected.id}` : 'Select a machine'}
    </button>
    {#if toast}
      <p class="toast">{toast}</p>
    {/if}

    <h3>Feed</h3>
    <div class="feed">
      {#if snap}
        {#each [...snap.events].reverse().slice(0, 12) as ev}
          <div class="ev">
            <strong>{ev.kind}</strong>
            {new Date(ev.time).toLocaleTimeString()} — {ev.message}
          </div>
        {/each}
      {/if}
    </div>
  </aside>

  <button
    type="button"
    class="grafana-tab"
    class:open={grafanaOpen}
    aria-expanded={grafanaOpen}
    aria-controls="grafana-drawer"
    onclick={() => (grafanaOpen = !grafanaOpen)}
  >
    Grafana
  </button>

  <aside id="grafana-drawer" class="grafana-drawer" class:open={grafanaOpen} aria-hidden={!grafanaOpen}>
    <div class="grafana-head">
      <strong>Grafana</strong>
      <a href="http://localhost:3000/d/kmc-overview/kill-my-cluster" target="_blank" rel="noreferrer"
        >Open ↗</a
      >
      <button type="button" class="close" onclick={() => (grafanaOpen = false)}>Close</button>
    </div>
    {#if grafanaOpen}
      <iframe
        class="grafana-frame"
        title="Kill My Cluster Grafana"
        src={GRAFANA_EMBED}
        loading="lazy"
        referrerpolicy="no-referrer"
      ></iframe>
    {/if}
  </aside>
</main>

<style>
  :global(html, body) {
    margin: 0;
    height: 100%;
    overflow: hidden;
    background: #0b0f14;
    color: #e8eef4;
    font-family: 'IBM Plex Sans', ui-sans-serif, system-ui, sans-serif;
  }

  main {
    position: relative;
    min-height: 100vh;
    min-height: 100dvh;
  }

  main.immersive {
    height: 100vh;
    height: 100dvh;
    overflow: hidden;
  }

  .stage {
    position: relative;
    min-height: 360px;
    background: #121a24;
  }

  .stage.fullscreen {
    position: absolute;
    inset: 0;
    min-height: 0;
    background: transparent;
    z-index: 0;
  }

  .loading {
    color: #7a8fa3;
    text-align: center;
    padding: 4rem 1rem;
  }

  .hud-top {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    z-index: 2;
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    padding: 1rem 1.25rem;
    pointer-events: none;
    background: linear-gradient(to bottom, rgba(11, 15, 20, 0.72), transparent);
  }

  h1 {
    margin: 0;
    font-size: 1.35rem;
    letter-spacing: -0.02em;
  }

  .live {
    font-size: 0.8rem;
    color: #7a8fa3;
  }

  .live.on {
    color: #3dd68c;
  }

  .hint {
    margin-left: auto;
    font-size: 0.72rem;
    color: #5a6f82;
  }

  .banner {
    position: absolute;
    top: 3.4rem;
    left: 50%;
    transform: translateX(-50%);
    z-index: 3;
    padding: 0.65rem 1rem;
    border-radius: 8px;
    background: rgba(255, 92, 92, 0.16);
    border: 1px solid rgba(255, 92, 92, 0.4);
    color: #ffb4b4;
    pointer-events: none;
  }

  .hud-panel {
    position: absolute;
    top: 4.25rem;
    right: 1rem;
    z-index: 2;
    width: min(280px, calc(100vw - 2rem));
    max-height: calc(100vh - 5.5rem);
    overflow: auto;
    background: rgba(18, 26, 36, 0.88);
    border: 1px solid #1e2d3d;
    border-radius: 14px;
    padding: 1rem 1.1rem;
    backdrop-filter: blur(10px);
  }

  .stats {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 0.55rem;
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
    font-size: 1.02rem;
  }

  h3 {
    margin: 1rem 0 0.4rem;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #7a8fa3;
  }

  button {
    font: inherit;
    font-size: 0.9rem;
    padding: 0.65rem 0.75rem;
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
    width: 100%;
    border-color: rgba(255, 92, 92, 0.5);
    color: #ffb4b4;
    font-weight: 650;
  }

  .toast {
    min-height: 1.2em;
    color: #7a8fa3;
    font-size: 0.82rem;
    margin: 0.65rem 0 0;
  }

  .feed {
    max-height: 180px;
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

  .grafana-tab {
    position: absolute;
    top: 50%;
    left: 0;
    z-index: 4;
    transform: translateY(-50%);
    writing-mode: vertical-rl;
    text-orientation: mixed;
    padding: 1rem 0.45rem;
    border-radius: 0 10px 10px 0;
    border: 1px solid #1e2d3d;
    border-left: 0;
    background: rgba(18, 26, 36, 0.92);
    color: #f0c14b;
    font-size: 0.78rem;
    font-weight: 650;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    backdrop-filter: blur(8px);
  }

  .grafana-tab.open {
    left: min(420px, 92vw);
  }

  .grafana-drawer {
    position: absolute;
    top: 0;
    left: 0;
    z-index: 3;
    width: min(420px, 92vw);
    height: 100%;
    display: flex;
    flex-direction: column;
    background: rgba(11, 15, 20, 0.96);
    border-right: 1px solid #1e2d3d;
    transform: translateX(-105%);
    transition: transform 0.22s ease;
    pointer-events: none;
  }

  .grafana-drawer.open {
    transform: translateX(0);
    pointer-events: auto;
  }

  .grafana-head {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.85rem 1rem;
    border-bottom: 1px solid #1e2d3d;
  }

  .grafana-head a {
    color: #f0c14b;
    text-decoration: none;
    font-size: 0.82rem;
  }

  .grafana-head .close {
    margin-left: auto;
    padding: 0.35rem 0.6rem;
    font-size: 0.78rem;
  }

  .grafana-frame {
    flex: 1;
    width: 100%;
    border: 0;
    background: #0b0f14;
    min-height: 0;
  }

  @media (max-width: 720px) {
    .hud-panel {
      top: auto;
      bottom: 1rem;
      right: 1rem;
      left: 1rem;
      width: auto;
      max-height: 38vh;
    }

    .hint {
      display: none;
    }
  }
</style>
