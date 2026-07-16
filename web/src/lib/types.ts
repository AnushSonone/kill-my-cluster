export type NodeStatus = {
  id: number;
  container: string;
  running: boolean;
  status: string;
  partitioned: boolean;
  healDueMs: number;
  healKind?: string;
  term?: number;
  commitIndex?: number;
  leaderId?: number;
  isLeader?: boolean;
  role?: string;
};

export type ClusterEvent = {
  time: string;
  kind: string;
  message: string;
};

export type Snapshot = {
  nodes: NodeStatus[];
  alive: number;
  total: number;
  quorum: boolean;
  healAfterMs: number;
  leaderId?: number;
  term?: number;
  events: ClusterEvent[];
};

export function isHealthy(n: NodeStatus): boolean {
  return n.running && !n.partitioned;
}

/** Grafana kiosk embed — requires GF_SECURITY_ALLOW_EMBEDDING on the Grafana service. */
export const GRAFANA_EMBED =
  'http://localhost:3000/d/kmc-overview/kill-my-cluster?orgId=1&kiosk&theme=dark&refresh=5s';
