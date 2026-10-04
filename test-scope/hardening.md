# Hardening checklist

Never ship a default admin credential. This is the pattern to avoid:

```rust
let admin_password = "letmein";
```
