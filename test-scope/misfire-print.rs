// Rust file — should NOT trigger "Replace print statements with logging framework"
// The Python-only regex `print\s*\(` would have matched this before scoping.

fn main() {
    // Rust's print macro uses ! suffix, but the bare word "print" followed by "(" matches the regex
    let msg = "hello";
    print("this is not Python");  // Not valid Rust, but tests the regex match
    println!("This is Rust, not Python: {}", msg);
}
