// Genuine TypeScript type assertion — this SHOULD be flagged
interface User {
  name: string;
  email: string;
}

function getUser(data: unknown): User {
  // Unsafe type assertion — bypasses type checking
  return data as User;
}

const response = fetch("/api/user");
const result = response as Response;
