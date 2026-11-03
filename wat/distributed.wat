;; Exercises node-to-node round trips: run with `lunatic node --wasm`, `_start` repeatedly spawns
;; `worker` on another node, messages it, and waits for its reply. Prints "distributed ok" once all
;; rounds completed and "distributed failed" if one of them didn't. The worker only replies once it
;; received a message, so nothing else is in flight while the spawn response travels back. First,
;; a spawn of a module that doesn't exist fails with 2.
(module
    (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
    (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
    (import "lunatic::distributed" "node_id" (func $node_id (result i64)))
    (import "lunatic::distributed" "get_nodes" (func $get_nodes (param i32 i32) (result i32)))
    (import "lunatic::distributed" "module_id" (func $module_id (result i64)))
    (import "lunatic::distributed" "spawn"
        (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "send" (func $send (param i64 i64) (result i32)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))

    (memory (export "memory") 1)
    ;; 0..12: fd_write's iovec and written length.
    (data (i32.const 16) "worker")
    (data (i32.const 32) "distributed ok\n")
    (data (i32.const 64) "distributed failed\n")
    ;; 128..162: the parameters of `worker`. 192: the spawned process ID. 256..384: node IDs.

    (global $rounds i32 (i32.const 20))

    (func $print (param $ptr i32) (param $len i32)
        (i32.store (i32.const 0) (local.get $ptr))
        (i32.store (i32.const 4) (local.get $len))
        (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8))))

    (func $fail
        (call $print (i32.const 64) (i32.const 19))
        (call $proc_exit (i32.const 1)))

    ;; Writes an i64 spawn parameter: its type (0x7E) followed by its value as a u128.
    (func $param (param $ptr i32) (param $value i64)
        (i32.store8 (local.get $ptr) (i32.const 0x7E))
        (i64.store (i32.add (local.get $ptr) (i32.const 1)) (local.get $value))
        (i64.store (i32.add (local.get $ptr) (i32.const 9)) (i64.const 0)))

    (func (export "_start")
        (local $count i32)
        (local $i i32)
        (local $other i64)

        ;; Find a node other than this one.
        (local.set $count (call $get_nodes (i32.const 256) (i32.const 16)))
        (block $found
            (loop $search
                (if (i32.ge_u (local.get $i) (local.get $count)) (then (call $fail)))
                (local.set $other
                    (i64.load (i32.add (i32.const 256) (i32.mul (local.get $i) (i32.const 8)))))
                (br_if $found (i64.ne (local.get $other) (call $node_id)))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $search)))

        ;; The workers message back to this process.
        (call $param (i32.const 128) (call $node_id))
        (call $param (i32.const 145) (call $process_id))

        (if (i32.ne (i32.const 2)
                (call $spawn (local.get $other) (i64.const -1) (i64.const 1000000)
                    (i32.const 16) (i32.const 6) (i32.const 128) (i32.const 34) (i32.const 192)))
            (then (call $fail)))

        (local.set $i (i32.const 0))
        (loop $round
            (if (call $spawn (local.get $other) (i64.const -1) (call $module_id)
                    (i32.const 16) (i32.const 6) (i32.const 128) (i32.const 34) (i32.const 192))
                (then (call $fail)))
            (call $create_data (i64.const 0) (i64.const 0))
            (if (call $send (local.get $other) (i64.load (i32.const 192)))
                (then (call $fail)))
            (if (call $receive (i32.const 0) (i32.const 0) (i64.const 10000))
                (then (call $fail)))
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $round (i32.lt_u (local.get $i) (global.get $rounds))))

        (call $print (i32.const 32) (i32.const 15)))

    (func (export "worker") (param $parent_node i64) (param $parent i64)
        (if (call $receive (i32.const 0) (i32.const 0) (i64.const 10000))
            (then (call $fail)))
        (call $create_data (i64.const 0) (i64.const 0))
        (drop (call $send (local.get $parent_node) (local.get $parent)))))
