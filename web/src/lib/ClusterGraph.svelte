<script lang="ts">
  import type { NodeStatus } from '$lib/types';
  import { isHealthy } from '$lib/types';
  import { onMount } from 'svelte';

  interface Props {
    nodes: NodeStatus[];
    selectedId: number | null;
    onselect: (id: number) => void;
  }

  let { nodes, selectedId, onselect }: Props = $props();

  const size = 560;
  const cx = size / 2;
  const cy = size / 2;
  const R_HEALTHY = 175;
  const R_DOWN = 248;

  type Pt = { id: number; x: number; y: number; node: NodeStatus; healSec: number | null };

  /** Mirror of props — kept in $state so the frame loop always sees kills/heals. */
  let live = $state<NodeStatus[]>([]);
  let layout = $state<Pt[]>([]);
  let edges = $state<{ x1: number; y1: number; x2: number; y2: number; key: string }[]>([]);
  const pos = new Map<number, { x: number; y: number }>();
  /** Local heal deadlines (ms epoch) so the caption ticks every frame, not every SSE. */
  const healAt = new Map<number, number>();

  $effect(() => {
    live = nodes;
  });

  function syncHealDeadline(node: NodeStatus, now: number) {
    // Sticky per down-cycle: arm once, never let SSE push time back up.
    if (isHealthy(node)) {
      healAt.delete(node.id);
      return;
    }
    if (healAt.has(node.id)) return;
    if (node.healDueMs > 0) {
      healAt.set(node.id, now + node.healDueMs);
    }
  }

  function healSeconds(now: number, id: number): number | null {
    const end = healAt.get(id);
    if (end == null) return null;
    return Math.max(0, (end - now) / 1000);
  }

  function targetFor(node: NodeStatus, healthy: NodeStatus[], down: NodeStatus[]) {
    const ok = isHealthy(node);
    const list = ok ? healthy : down;
    const i = list.findIndex((n) => n.id === node.id);
    const n = Math.max(list.length, 1);
    const r = ok ? R_HEALTHY : R_DOWN;
    const a = (2 * Math.PI * i) / n - Math.PI / 2;
    return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
  }

  function tick(dt: number) {
    const now = Date.now();
    const sorted = [...live].sort((a, b) => a.id - b.id);
    const healthy = sorted.filter(isHealthy);
    const down = sorted.filter((n) => !isHealthy(n));
    const ease = Math.min(1, dt * 6);
    const pts: Pt[] = [];

    for (const node of sorted) {
      syncHealDeadline(node, now);
      const target = targetFor(node, healthy, down);
      const prev = pos.get(node.id) ?? target;
      const x = prev.x + (target.x - prev.x) * ease;
      const y = prev.y + (target.y - prev.y) * ease;
      pos.set(node.id, { x, y });
      pts.push({ id: node.id, node, x, y, healSec: healSeconds(now, node.id) });
    }
    for (const id of [...pos.keys()]) {
      if (!sorted.some((n) => n.id === id)) {
        pos.delete(id);
        healAt.delete(id);
      }
    }
    layout = pts;

    const healthyPts = pts.filter((p) => isHealthy(p.node));
    const eds: typeof edges = [];
    for (let i = 0; i < healthyPts.length; i++) {
      for (let j = i + 1; j < healthyPts.length; j++) {
        const a = healthyPts[i];
        const b = healthyPts[j];
        const dist = Math.min(j - i, healthyPts.length - (j - i));
        if (healthyPts.length > 3 && dist > 2) continue;
        eds.push({ x1: a.x, y1: a.y, x2: b.x, y2: b.y, key: `${a.id}-${b.id}` });
      }
    }
    edges = eds;
  }

  onMount(() => {
    let raf = 0;
    let last = performance.now();
    const loop = (now: number) => {
      const dt = Math.min(0.05, (now - last) / 1000);
      last = now;
      tick(dt);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  });
</script>

<svg class="graph" viewBox="0 0 {size} {size}" role="img" aria-label="Raft machine mesh">
  <defs>
    <radialGradient id="bgGlow" cx="50%" cy="50%" r="50%">
      <stop offset="0%" stop-color="#1a3048" stop-opacity="0.9" />
      <stop offset="100%" stop-color="#0b0f14" stop-opacity="0" />
    </radialGradient>
    <filter id="softGlow" x="-40%" y="-40%" width="180%" height="180%">
      <feGaussianBlur stdDeviation="4" result="blur" />
      <feMerge>
        <feMergeNode in="blur" />
        <feMergeNode in="SourceGraphic" />
      </feMerge>
    </filter>
  </defs>

  <circle cx={cx} cy={cy} r={220} fill="url(#bgGlow)" />

  {#each edges as e (e.key)}
    <line class="edge live" x1={e.x1} y1={e.y1} x2={e.x2} y2={e.y2} />
  {/each}

  {#each layout as { node, x, y, healSec } (node.id)}
    {@const healthy = isHealthy(node)}
    {@const selected = selectedId === node.id}
    <g
      class="machine"
      class:healthy
      class:dead={!node.running}
      class:partitioned={node.running && node.partitioned}
      class:leader={!!node.isLeader}
      class:selected
      transform="translate({x}, {y})"
      role="button"
      tabindex="0"
      onclick={() => onselect(node.id)}
      onkeydown={(ev) => {
        if (ev.key === 'Enter' || ev.key === ' ') onselect(node.id);
      }}
    >
      {#if node.isLeader}
        <circle class="halo leader-halo" r="32" />
      {:else if healthy}
        <circle class="halo" r="28" />
      {/if}
      <circle class="disk" r="22" filter={selected || node.isLeader ? 'url(#softGlow)' : undefined} />
      <text class="label" text-anchor="middle" dominant-baseline="central">{node.id}</text>
      {#if node.isLeader}
        <text class="caption" text-anchor="middle" y="36">leader</text>
      {:else if healSec != null}
        <text class="caption" text-anchor="middle" y="36">{healSec.toFixed(1)}s</text>
      {/if}
    </g>
  {/each}
</svg>

<style>
  .graph {
    width: 100%;
    max-width: 640px;
    height: auto;
    display: block;
    margin: 0 auto;
  }

  .edge.live {
    stroke: #5eb3ff;
    stroke-width: 2;
    opacity: 0.55;
    animation: pulse-edge 2.2s ease-in-out infinite;
  }

  @keyframes pulse-edge {
    0%,
    100% {
      opacity: 0.28;
    }
    50% {
      opacity: 0.8;
    }
  }

  .machine {
    cursor: pointer;
  }

  .disk {
    fill: #121a24;
    stroke: #3a4f66;
    stroke-width: 2;
    transition: stroke 0.25s, fill 0.25s, opacity 0.25s;
  }

  .halo {
    fill: rgba(61, 214, 140, 0.12);
    stroke: none;
  }

  .leader-halo {
    fill: rgba(240, 193, 75, 0.18);
    animation: halo-pulse 1.4s ease-in-out infinite;
  }

  @keyframes halo-pulse {
    0%,
    100% {
      opacity: 0.45;
    }
    50% {
      opacity: 1;
    }
  }

  .machine.healthy .disk {
    stroke: #3dd68c;
    fill: #0f1c18;
  }

  .machine.leader .disk {
    stroke: #f0c14b;
    stroke-width: 3;
    fill: #1a1810;
  }

  .machine.dead .disk {
    stroke: #ff5c5c;
    fill: #1a1012;
    opacity: 0.75;
  }

  .machine.partitioned .disk {
    stroke: #b794f6;
    fill: #16122a;
  }

  .machine.selected .disk {
    stroke-width: 3.5;
  }

  .label {
    fill: #e8eef4;
    font-size: 14px;
    font-weight: 650;
    font-family: ui-sans-serif, system-ui, sans-serif;
    pointer-events: none;
  }

  .caption {
    fill: #7a8fa3;
    font-size: 10px;
    font-family: ui-sans-serif, system-ui, sans-serif;
    pointer-events: none;
  }

  .machine.leader .caption {
    fill: #f0c14b;
  }
</style>
