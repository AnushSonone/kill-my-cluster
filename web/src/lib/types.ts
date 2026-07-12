export type NodeStatus = {
  id: number;
  container: string;
  running: boolean;
  status: string;
  partitioned: boolean;
  healDueMs: number;
  healKind?: string;
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
  events: ClusterEvent[];
};

export function isHealthy(n: NodeStatus): boolean {
  return n.running && !n.partitioned;
}
