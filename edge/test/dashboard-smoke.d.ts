// Query-suffixed imports of the dashboard module: each distinct suffix gives a
// test its own fresh module instance (bootstrap side effects run once per import).
declare module '../public/dashboard.js?*' {
  export {};
}
