<script lang="ts">
	import { T, useTask } from '@threlte/core';
	import { HTML, OrbitControls, Stars, interactivity } from '@threlte/extras';
	import type { NodeStatus } from '$lib/types';
	import { isHealthy } from '$lib/types';
	import type { MeshStandardMaterial } from 'three';

	interface Props {
		nodes: NodeStatus[];
		selectedId: number | null;
		onselect: (id: number) => void;
	}

	let { nodes, selectedId, onselect }: Props = $props();

	interactivity();

	const R_HEALTHY = 2.35;
	const R_DOWN = 3.55;
	/** Blink-out duration before the machine settles on the outer ring. */
	const DEATH_MS = 900;

	type Pt = {
		id: number;
		x: number;
		y: number;
		z: number;
		node: NodeStatus;
		healSec: number | null;
		opacity: number;
		scaleMul: number;
		dying: boolean;
		showLabel: boolean;
	};

	let live = $state<NodeStatus[]>([]);
	let layout = $state<Pt[]>([]);
	let edges = $state<{ key: string; points: Float32Array; alive: boolean }[]>([]);
	let pulse = $state(0);

	const pos = new Map<number, { x: number; y: number; z: number }>();
	const healAt = new Map<number, number>();
	const prevHealthy = new Map<number, boolean>();
	const deathFx = new Map<number, { started: number; x: number; y: number; z: number }>();

	$effect(() => {
		live = nodes;
	});

	function syncHealDeadline(node: NodeStatus, now: number) {
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
		return {
			x: r * Math.cos(a),
			y: ok ? 0.2 : -0.55,
			z: r * Math.sin(a)
		};
	}

	function machineColor(node: NodeStatus, selected: boolean): string {
		if (node.isLeader) return '#f0c14b';
		if (!node.running) return '#ff5c5c';
		if (node.partitioned) return '#b794f6';
		if (selected) return '#7ec8ff';
		return '#3dd68c';
	}

	function blockSelect(ev: Event) {
		ev.preventDefault();
	}

	useTask((dt) => {
		const now = Date.now();
		pulse = (pulse + dt) % (Math.PI * 2);
		const sorted = [...live].sort((a, b) => a.id - b.id);
		const healthy = sorted.filter(isHealthy);
		const down = sorted.filter((n) => !isHealthy(n));
		const ease = Math.min(1, dt * 6);
		const pts: Pt[] = [];

		for (const node of sorted) {
			syncHealDeadline(node, now);
			const ok = isHealthy(node);
			const was = prevHealthy.get(node.id);
			const target = targetFor(node, healthy, down);
			const prev = pos.get(node.id) ?? target;

			if (was === true && !ok) {
				deathFx.set(node.id, { started: now, x: prev.x, y: prev.y, z: prev.z });
			}
			if (ok) deathFx.delete(node.id);
			prevHealthy.set(node.id, ok);

			const fx = deathFx.get(node.id);
			let x: number;
			let y: number;
			let z: number;
			let opacity = ok ? 1 : 0.72;
			let scaleMul = 1;
			let dying = false;
			let showLabel = true;

			if (fx) {
				const t = (now - fx.started) / DEATH_MS;
				if (t < 1) {
					dying = true;
					// Hold at kill position while blinking out.
					x = fx.x;
					y = fx.y;
					z = fx.z;
					pos.set(node.id, { x, y, z });
					const blink = Math.sin(t * Math.PI * 14) > 0 ? 1 : 0.08;
					opacity = blink * Math.max(0, 1 - t * t);
					scaleMul = Math.max(0.02, 1 - t);
					showLabel = opacity > 0.25;
				} else {
					deathFx.delete(node.id);
					x = prev.x + (target.x - prev.x) * ease;
					y = prev.y + (target.y - prev.y) * ease;
					z = prev.z + (target.z - prev.z) * ease;
					pos.set(node.id, { x, y, z });
				}
			} else {
				x = prev.x + (target.x - prev.x) * ease;
				y = prev.y + (target.y - prev.y) * ease;
				z = prev.z + (target.z - prev.z) * ease;
				pos.set(node.id, { x, y, z });
			}

			pts.push({
				id: node.id,
				node,
				x,
				y,
				z,
				healSec: healSeconds(now, node.id),
				opacity,
				scaleMul,
				dying,
				showLabel
			});
		}
		for (const id of [...pos.keys()]) {
			if (!sorted.some((n) => n.id === id)) {
				pos.delete(id);
				healAt.delete(id);
				prevHealthy.delete(id);
				deathFx.delete(id);
			}
		}
		layout = pts;

		// Full mesh: every pair of machines gets an edge.
		const eds: typeof edges = [];
		for (let i = 0; i < pts.length; i++) {
			for (let j = i + 1; j < pts.length; j++) {
				const a = pts[i];
				const b = pts[j];
				// Hide edges while a machine is blinking out.
				if (a.dying || b.dying || a.opacity < 0.05 || b.opacity < 0.05) continue;
				eds.push({
					key: `${a.id}-${b.id}`,
					alive: isHealthy(a.node) && isHealthy(b.node),
					points: new Float32Array([a.x, a.y, a.z, b.x, b.y, b.z])
				});
			}
		}
		edges = eds;
	});
</script>

<T.PerspectiveCamera makeDefault position={[0, 6.2, 8.4]} fov={42} near={0.1} far={200}>
	<OrbitControls
		enableDamping
		dampingFactor={0.08}
		enablePan={false}
		enableZoom
		zoomSpeed={1.15}
		minDistance={2}
		maxDistance={40}
		minPolarAngle={0.2}
		maxPolarAngle={Math.PI / 2 - 0.08}
		target={[0, 0, 0]}
	/>
</T.PerspectiveCamera>

<Stars
	radius={80}
	depth={60}
	count={4500}
	factor={3.2}
	saturation={0.15}
	lightness={0.85}
	fade
	rounded
	speed={0.35}
	opacity={0.9}
/>

<T.AmbientLight intensity={0.4} />
<T.DirectionalLight position={[5, 10, 4]} intensity={1.05} />
<T.DirectionalLight position={[-4, 3, -5]} intensity={0.3} color="#8eb6ff" />

<!-- Soft floor disc for depth -->
<T.Mesh rotation.x={-Math.PI / 2} position={[0, -0.9, 0]}>
	<T.CircleGeometry args={[4.4, 64]} />
	<T.MeshStandardMaterial color="#0e1620" transparent opacity={0.55} />
</T.Mesh>

{#each edges as e (e.key)}
	{@const edgeOpacity = e.alive
		? 0.32 + 0.4 * (0.5 + 0.5 * Math.sin(pulse * 2.2))
		: 0.12}
	<T.Line>
		<T.BufferGeometry>
			<T.BufferAttribute attach="attributes-position" count={2} array={e.points} itemSize={3} />
		</T.BufferGeometry>
		<T.LineBasicMaterial
			color={e.alive ? '#5eb3ff' : '#5a3a3a'}
			transparent
			opacity={edgeOpacity}
		/>
	</T.Line>
{/each}

{#each layout as { node, x, y, z, healSec, opacity, scaleMul, dying, showLabel } (node.id)}
	{@const selected = selectedId === node.id}
	{@const color = dying ? '#ff5c5c' : machineColor(node, selected)}
	{@const leaderPulse = node.isLeader && !dying ? 1 + 0.06 * Math.sin(pulse * 3.2) : 1}
	{@const scale = (selected && !dying ? 1.12 : 1) * leaderPulse * scaleMul}
	{#if opacity > 0.02}
		<T.Group position.x={x} position.y={y} position.z={z} scale={[scale, scale, scale]}>
			{#if node.isLeader && !dying}
				<T.Mesh>
					<T.SphereGeometry args={[0.52, 24, 24]} />
					<T.MeshStandardMaterial
						color="#f0c14b"
						transparent
						opacity={(0.14 + 0.1 * (0.5 + 0.5 * Math.sin(pulse * 3.2))) * opacity}
						emissive="#f0c14b"
						emissiveIntensity={0.35}
					/>
				</T.Mesh>
			{/if}
			<T.Mesh
				onclick={(ev) => {
					ev.stopPropagation();
					if (!dying) onselect(node.id);
				}}
				onpointerenter={(ev) => {
					const mat = (ev.object as { material?: MeshStandardMaterial }).material;
					if (mat && !dying) mat.emissiveIntensity = 0.55;
				}}
				onpointerleave={(ev) => {
					const mat = (ev.object as { material?: MeshStandardMaterial }).material;
					if (mat) mat.emissiveIntensity = node.isLeader ? 0.4 : 0.15;
				}}
			>
				<T.SphereGeometry args={[0.38, 28, 28]} />
				<T.MeshStandardMaterial
					color={color}
					emissive={color}
					emissiveIntensity={dying ? 0.8 : node.isLeader ? 0.4 : selected ? 0.3 : 0.15}
					transparent
					opacity={opacity}
					roughness={0.45}
					metalness={0.25}
				/>
			</T.Mesh>
			{#if showLabel}
				<HTML center occlude={false}>
					<button
						type="button"
						class="tag"
						class:leader={!!node.isLeader && !dying}
						class:dead={!node.running}
						class:selected
						tabindex={dying ? -1 : 0}
						onclick={() => {
							if (!dying) onselect(node.id);
						}}
						onmousedown={blockSelect}
						ondblclick={blockSelect}
						onselectstart={blockSelect}
						ondragstart={blockSelect}
					>
						<span class="id">M{node.id}</span>
						{#if node.isLeader && !dying}
							<span class="cap">leader</span>
						{:else if healSec != null && !dying}
							<span class="cap">{healSec.toFixed(1)}s</span>
						{/if}
					</button>
				</HTML>
			{/if}
		</T.Group>
	{/if}
{/each}

<style>
	.tag {
		display: flex;
		flex-direction: column;
		align-items: center;
		gap: 0.05rem;
		padding: 0.15rem 0.35rem;
		border: 0;
		background: transparent;
		color: #e8eef4;
		cursor: pointer;
		font-family: ui-sans-serif, system-ui, sans-serif;
		pointer-events: auto;
		transform: translateY(1.6rem);
		user-select: none;
		-webkit-user-select: none;
		-webkit-touch-callout: none;
		-webkit-user-drag: none;
	}

	.id,
	.cap {
		user-select: none;
		-webkit-user-select: none;
		pointer-events: none;
	}

	.id {
		font-size: 12px;
		font-weight: 700;
		text-shadow: 0 1px 3px rgba(0, 0, 0, 0.85);
	}

	.cap {
		font-size: 10px;
		color: #7a8fa3;
		text-shadow: 0 1px 3px rgba(0, 0, 0, 0.85);
	}

	.tag.leader .cap {
		color: #f0c14b;
	}

	.tag.dead .cap {
		color: #ffb4b4;
	}

	.tag.selected .id {
		text-decoration: underline;
		text-underline-offset: 2px;
	}
</style>
