;; Run with `lunatic node --wasm`: monitors a process on another node, which waits forever. Prints
;; "monitoring" once it monitors the process, then "node down ok" once the monitor fired because
;; the other node went down and spawns and sends to it fail, and "node down failed" otherwise. Spawns
;; and sends to a node that never existed fail too.
(module
    (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
    (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
    (import "lunatic::distributed" "node_id" (func $node_id (result i64)))
    (import "lunatic::distributed" "get_nodes" (func $get_nodes (param i32 i32) (result i32)))
    (import "lunatic::distributed" "module_id" (func $module_id (result i64)))
    (import "lunatic::distributed" "spawn"
        (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "get_or_spawn"
        (func $get_or_spawn
            (param i32 i32 i64 i64 i64 i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "monitor" (func $monitor (param i64 i64)))
    (import "lunatic::distributed" "send" (func $send (param i64 i64) (result i32)))
    (import "lunatic::distributed" "send_receive_skip_search"
        (func $send_receive_skip_search (param i64 i64 i64 i64) (result i32)))
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::registry" "get" (func $registry_get (param i32 i32 i32 i32) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))

    (memory (export "memory") 1)
    ;; 0..12: fd_write's iovec and written length.
    (data (i32.const 16) "waiter")
    (data (i32.const 24) "name")
    (data (i32.const 32) "monitoring\n")
    (data (i32.const 48) "node down ok\n")
    (data (i32.const 64) "node down failed\n")
    ;; 192: the spawned process ID. 200: a node ID. 256..384: node IDs.

    (func $print (param $ptr i32) (param $len i32)
        (i32.store (i32.const 0) (local.get $ptr))
        (i32.store (i32.const 4) (local.get $len))
        (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8))))

    (func $fail
        (call $print (i32.const 64) (i32.const 17))
        (call $proc_exit (i32.const 1)))

    ;; Spawning on `node` fails because it isn't in the cluster (1), and leaves "name" free. Sends
    ;; to it fail (2).
    (func $requests_fail (param $node i64)
        (if (i32.ne (i32.const 1)
                (call $spawn (local.get $node) (i64.const -1) (call $module_id)
                    (i32.const 16) (i32.const 6) (i32.const 128) (i32.const 0) (i32.const 192)))
            (then (call $fail)))
        (if (i32.ne (i32.const 1)
                (call $get_or_spawn (i32.const 24) (i32.const 4) (local.get $node) (i64.const -1)
                    (call $module_id) (i32.const 16) (i32.const 6) (i32.const 128) (i32.const 0)
                    (i32.const 200) (i32.const 192)))
            (then (call $fail)))
        (if (i32.ne (i32.const 1)
                (call $registry_get (i32.const 24) (i32.const 4) (i32.const 200) (i32.const 192)))
            (then (call $fail)))
        (call $create_data (i64.const 0) (i64.const 0))
        (if (i32.ne (i32.const 2) (call $send (local.get $node) (i64.const 1)))
            (then (call $fail)))
        (call $create_data (i64.const 0) (i64.const 0))
        (if (i32.ne (i32.const 2)
                (call $send_receive_skip_search (local.get $node) (i64.const 1) (i64.const 0)
                    (i64.const 5000)))
            (then (call $fail))))

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
        (call $requests_fail (i64.const 1000000))

        (if (call $spawn (local.get $other) (i64.const -1) (call $module_id)
                (i32.const 16) (i32.const 6) (i32.const 128) (i32.const 0) (i32.const 192))
            (then (call $fail)))
        (call $monitor (local.get $other) (i64.load (i32.const 192)))
        (call $print (i32.const 32) (i32.const 11))
        (if (i32.ne (call $receive (i32.const 0) (i32.const 0) (i64.const 30000)) (i32.const 2))
            (then (call $fail)))
        (call $requests_fail (local.get $other))
        (call $print (i32.const 48) (i32.const 13)))

    (func (export "waiter")
        (drop (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))))
