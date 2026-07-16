/** True when the browser can create a WebGL (or WebGL2) context. */
export function supportsWebGL(): boolean {
	if (typeof document === 'undefined') return false;
	try {
		const canvas = document.createElement('canvas');
		return !!(
			canvas.getContext('webgl2') ||
			canvas.getContext('webgl') ||
			canvas.getContext('experimental-webgl')
		);
	} catch {
		return false;
	}
}

/** Prefer 3D unless WebGL is missing or `?view=svg` is set. */
export function preferCluster3D(search = typeof location !== 'undefined' ? location.search : ''): boolean {
	const params = new URLSearchParams(search);
	if (params.get('view') === 'svg') return false;
	if (params.get('view') === '3d') return supportsWebGL();
	return supportsWebGL();
}
