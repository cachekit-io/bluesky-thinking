// Query-suffixed imports of the dashboard module: each distinct suffix gives a
// test its own fresh module instance (bootstrap side effects run once per import).
// Ambient module names must be non-relative (TS2436), so match on the suffix.
declare module '*?smoke' {
  export {};
}
declare module '*?nan' {
  export {};
}
