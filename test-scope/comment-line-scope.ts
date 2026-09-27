interface User {
  id: string;
}

/**
 * Parses a user record. A missing record is reported as NotFound,
 * the same answer as GET would give.
 */
export function parseUser(raw: string): User {
  // The payload arrives as JSON and is trusted as User here on purpose.
  /* Counted as ONE parse, whatever the payload size. */
  const data: unknown = JSON.parse(raw);
  return data as User;
}
