Rust Redis Replication

This is a toy implementation of Redis replication in Rust. It's a work in progress and not ready for production use.

TODO:
- split out Reader and Writer for testability
---- Trait for read and write frame
- figure out a different approach for static subscribers
----- how to publish and add subscribers?
- only allow missing \r\n for bulk strings coming from "main" server
- split "apply" into "apply" and "respond" or apply takes a flag for "respond"??