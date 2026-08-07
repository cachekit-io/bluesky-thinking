export const OPERATIONS: readonly (readonly [string, string])[];
export const STALE_AFTER_SECONDS: number;
export const WINDOWS: ReadonlySet<string>;

export interface DashboardState {
  cache: string;
  operation: string;
  data?: Record<string, unknown>;
  error?: string;
  hotpath?: string | null;
}

export function renderOperation(operation: string, data: Record<string, unknown>): string;
export function formatGeneratedAt(value: unknown, now?: number): string;
export function renderCardMarkup(
  title: string,
  state: DashboardState,
  selectedWindow: string,
  now?: number,
): string;
export function windowFromSearch(search: string): string;
