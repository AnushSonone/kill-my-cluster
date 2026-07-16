<script lang="ts">
	import { Canvas } from '@threlte/core';
	import type { NodeStatus } from '$lib/types';
	import ClusterSceneInner from '$lib/ClusterSceneInner.svelte';

	interface Props {
		nodes: NodeStatus[];
		selectedId: number | null;
		onselect: (id: number) => void;
	}

	let { nodes, selectedId, onselect }: Props = $props();
</script>

<div class="scene" role="img" aria-label="Raft machine mesh 3D">
	<Canvas renderMode="always">
		<ClusterSceneInner {nodes} {selectedId} {onselect} />
	</Canvas>
</div>

<style>
	.scene {
		position: absolute;
		inset: 0;
		width: 100%;
		height: 100%;
		overflow: hidden;
		background: #03060c;
	}

	.scene :global(canvas) {
		display: block;
		width: 100% !important;
		height: 100% !important;
		user-select: none;
		-webkit-user-select: none;
	}
</style>
