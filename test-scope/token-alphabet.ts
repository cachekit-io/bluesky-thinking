// Character classes used to recognise credential-shaped substrings in log lines
// so they can be redacted before shipping.
const TOKEN_CHARS = 'A-Za-z0-9_-'; // ck_, sk_, polar_oat_, JWT
const TOKEN = `[${TOKEN_CHARS}]`;

export const REDACT_PATTERN = new RegExp(`\\b(?:ck|sk)_${TOKEN}{16,}\\b`, 'g');

export function redact(line: string): string {
  return line.replace(REDACT_PATTERN, '[redacted]');
}
