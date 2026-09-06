# L4 Load Bearer

I wrote this out of an interest in low-level networking. I had no prior experience in the subject, and thus my implementation was quite inelegant and a challenge to read and make sense of (`proxy_ugly.rs`). Since writing this, I have learned the modern, elegant, "correct" way to write these in Rust with asynchronous libraries like `mio`, namely, breaking down read, write, and reregistration/cleanup into 3 "phases". I have written an elegant version and am building off of that as of now.
