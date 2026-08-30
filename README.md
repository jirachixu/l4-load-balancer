# L4 Load Bearer

I wrote this out of an interest in low-level networking. I had no prior experience in the subject, and thus my implementation is quite inelegant and is a challenge to read and make sense of. Since writing this, I have learned the modern, elegant, "correct" way to write these in Rust with asynchronous libraries like `mio`, namely, breaking down read, write, and reregistration/cleanup into 3 "phases". If/when I continue down this rabbit hole in the future, I will keep this in mind (and learn the best practices from the start!).
