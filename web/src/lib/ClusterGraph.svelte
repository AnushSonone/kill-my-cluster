<script lang="ts">
  import type { NodeStatus } from '$lib/types';
  import { isHealthy } from '$lib/types';

  interface Props {
    nodes: NodeStatus[];
    selectedId: number | null;
    onselect: (id: number) => void;
  }

  let { nodes, selectedId, onselect }: Props = $props();

  const size = 560;
  const cx = size / 2;
  const cy = size / 2;
  const radius = 200;

  function pos(i: number, n: number) {
    // Start at top (-90°)
    const angle = (2 * Math.PI * i) / n - Math.PI / 2;
    return {
      x: cx + radius * Math.cos(angle),
      y: cy + radius * Math.sin(angle)
    };
  }

  let layout = $derived.by(() => {
    const sorted = [...nodes].sort((a, b) => a.id - b.id);
    const n = Math.max(sorted.length, 1);
    return sorted.map((node, i) => ({ node, ...pos(i, n) }));
  });

  let edges = $derived.by(() => {
    const pts = layout;
    const out: { x1: number; y1: number; x2: number; y2: number; live: boolean; key: string }[] = [];
    for (let i = 0; i < pts.length; i++) {
      for (let j = i + 1; j < pts.length; j++) {
        const a = pts[i];
        const b = pts[j];
        // Ring neighbors + skip-1 chords keep the mesh readable at 7 nodes
        const dist = Math.min(j - i, pts.length - (j - i));
        if (dist > 2) continue;
        out.push({
          x1: a.x,
          y1: a.y,
          x2: b.x,
          y2: b.y,
          live: isHealthy(a.node) && isHealthy(b.node),
          key: `${a.node.id}-${b.node.id}`
        });
      }
    }
    return out;
  });
</script>

<svg class="graph" viewBox="0 0 {size} {size}" role="img" aria-label="Raft cluster graph">
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

  <circle cx={cx} cy={cy} r={radius + 36} fill="url(#bgGlow)" />

  {#each edges as e (e.key)}
    <line
      class="edge"
      class:live={e.live}
      x1={e.x1}
      y1={e.y1}
      x2={e.x2}
      y2={e.y2}
    />
  {/each}

  {#each layout as { node, x, y } (node.id)}
    {@const healthy = isHealthy(node)}
    {@const selected = selectedId === node.id}
    <g
      class="node"
      class:healthy
      class:dead={!node.running}
      class:partitioned={node.running && node.partitioned}
      class:selected
      transform="translate({x}, {y})"
      role="button"
      tabindex="0"
      onclick={() => onselect(node.id)}
      onkeydown={(ev) => {
        if (ev.key === 'Enter' || ev.key === ' ') onselect(node.id);
      }}
    >
      {#if healthy}
        <circle class="halo" r="28" />
      {/if}
      <circle class="disk" r="22" filter={selected ? 'url(#softGlow)' : undefined} />
      <text class="label" text-anchor="middle" dominant-baseline="central">
        {node.id}
      </text>
      {#if node.healDueMs > 0}
        <text class="heal" text-anchor="middle" y="36">
          {(node.healDueMs / 1000).toFixed(1)}s
        </text>
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

  .edge {
    stroke: #243447;
    stroke-width: 1.5;
    opacity: 0.45;
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
      opacity: 0.3;
    }
    50% {
      opacity: 0.75;
    }
  }

  .node {
    cursor: pointer;
  }

  .disk {
    fill: #121a24;
    stroke: #3a4f66;
    stroke-width: 2;
    transition: stroke 0.2s, fill 0.2s;
  }

  .halo {
    fill: rgba(61, 214, 140, 0.12);
    stroke: none;
  }

  .node.healthy .disk {
    stroke: #3dd68c;
    fill: #0f1c18;
  }

  .node.dead .disk {
    stroke: #ff5c5c;
    fill: #1a1012;
    opacity: 0.7;
  }

  .node.partitioned .disk {
    stroke: #b794f6;
    fill: #16122a;
  }

  .node.selected .disk {
    stroke-width: 3;
    stroke: #f0c14b;
  }

  .label {
    fill: #e8eef4;
    font-size: 14px;
    font-weight: 650;
    font-family: ui-sans-serif, system-ui, sans-serif;
    pointer-events: none;
  }

  .heal {
    fill: #f0c14b;
    font-size: 10px;
    font-family: ui-sans-serif, system-ui, sans-serif;
    pointer-events: none;
  }
</style>
