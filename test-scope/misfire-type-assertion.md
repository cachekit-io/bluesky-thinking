# Type Assertion Misfire Test

This markdown file contains text that previously triggered the TS-only
"Avoid unsafe type assertions" rule because the detector regex `\bas\s+[A-Z]`
matched prose with "as SomeWord".

Example: cast the value as ValueError to see the stack trace.
Another example: use it as String for display purposes.
Also: treat the response as Response before parsing.
