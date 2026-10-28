;; Run with `lunatic node --wasm`: 8 processes concurrently spawn a process named "the-one" on
;; another node, after a spawn under the name failed. Prints "named spawn ok" once all of them got
;; the same process, which started once, and "named spawn failed" otherwise.
(module
    (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
    (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
    (import "lunatic::distributed" "node_id" (func $node_id (result i64)))
    (import "lunatic::distributed" "get_nodes" (func $get_nodes (param i32 i32) (result i32)))
    (import "lunatic::distributed" "module_id" (func $module_id (result i64)))
    (import "lunatic::distributed" "get_or_spawn"
        (func $get_or_spawn
            (param i32 i32 i64 i64 i64 i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "send" (func $remote_send (param i64 i64) (result i32)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::process" "spawn"
        (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::registry" "get" (func $registry_get (param i32 i32 i32 i32) (result i32)))
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "write_data" (func $write_data (param i32 i32) (result i32)))
    (import "lunatic::message" "read_data" (func $read_data (param i32 i32) (result i32)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))

    (memory (export "memory") 1)
    ;; 0..12: fd_write's iovec and written length.
    (data (i32.const 16) "racer")
    (data (i32.const 24) "started")
    (data (i32.const 32) "the-one")
    (data (i32.const 48) "named spawn ok\n")
    (data (i32.const 64) "named spawn failed\n")
    ;; 128..162: the parameters of `racer`. 192..226: the parameters of `started`.
    ;; 240: a node ID. 248: a process ID. 256..384: node IDs. 400: a tag. 408: received data.
    ;; 416: the process "the-one".

    (global $racers i32 (i32.const 8))

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

    ;; Receives the next message tagged `tag`, returning 0 for data.
    (func $next (param $tag i64) (param $timeout i64) (result i32)
        (i64.store (i32.const 400) (local.get $tag))
        (call $receive (i32.const 400) (i32.const 1) (local.get $timeout)))

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

        ;; A spawn of a missing module fails, and leaves the name free.
        (call $param (i32.const 192) (call $node_id))
        (call $param (i32.const 209) (call $process_id))
        (if (i32.ne (i32.const 3)
                (call $get_or_spawn (i32.const 32) (i32.const 7) (local.get $other) (i64.const -1)
                    (i64.const 1000000) (i32.const 24) (i32.const 7) (i32.const 192) (i32.const 34)
                    (i32.const 240) (i32.const 248)))
            (then (call $fail)))
        (if (i32.ne (i32.const 1)
                (call $registry_get (i32.const 32) (i32.const 7) (i32.const 240) (i32.const 248)))
            (then (call $fail)))

        ;; The racers share this process' registry.
        (call $param (i32.const 128) (call $process_id))
        (call $param (i32.const 145) (local.get $other))
        (local.set $i (i32.const 0))
        (loop $spawning
            (if (call $spawn (i64.const 0) (i64.const -1) (i64.const -1)
                    (i32.const 16) (i32.const 5) (i32.const 128) (i32.const 34) (i32.const 192))
                (then (call $fail)))
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $spawning (i32.lt_u (local.get $i) (global.get $racers))))

        ;; Every racer got the same process.
        (local.set $i (i32.const 0))
        (loop $results
            (if (call $next (i64.const 1) (i64.const 10000)) (then (call $fail)))
            (drop (call $read_data (i32.const 408) (i32.const 8)))
            (if (i32.eqz (local.get $i))
                (then (i64.store (i32.const 416) (i64.load (i32.const 408)))))
            (if (i64.ne (i64.load (i32.const 408)) (i64.load (i32.const 416)))
                (then (call $fail)))
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $results (i32.lt_u (local.get $i) (global.get $racers))))

        ;; It started once.
        (if (call $next (i64.const 2) (i64.const 10000)) (then (call $fail)))
        (if (i32.ne (call $next (i64.const 2) (i64.const 500)) (i32.const 9027))
            (then (call $fail)))

        ;; And it's registered.
        (if (call $registry_get (i32.const 32) (i32.const 7) (i32.const 240) (i32.const 248))
            (then (call $fail)))
        (if (i64.ne (i64.load (i32.const 240)) (local.get $other)) (then (call $fail)))
        (if (i64.ne (i64.load (i32.const 248)) (i64.load (i32.const 416))) (then (call $fail)))

        (call $print (i32.const 48) (i32.const 15)))

    ;; Gets or spawns "the-one" on node `other`, and sends its process ID to the parent (tag 1).
    (func (export "racer") (param $parent i64) (param $other i64)
        (local $result i32)
        (call $param (i32.const 192) (call $node_id))
        (call $param (i32.const 209) (local.get $parent))
        (local.set $result
            (call $get_or_spawn (i32.const 32) (i32.const 7) (local.get $other) (i64.const -1)
                (call $module_id) (i32.const 24) (i32.const 7) (i32.const 192) (i32.const 34)
                (i32.const 240) (i32.const 248)))
        (if (i32.and (i32.ne (local.get $result) (i32.const 0)) (i32.ne (local.get $result) (i32.const 2)))
            (then (call $fail)))
        (if (i64.ne (i64.load (i32.const 240)) (local.get $other)) (then (call $fail)))
        (call $create_data (i64.const 1) (i64.const 8))
        (drop (call $write_data (i32.const 248) (i32.const 8)))
        (drop (call $send (local.get $parent))))

    ;; Tells the parent it started (tag 2), and stays alive.
    (func (export "started") (param $parent_node i64) (param $parent i64)
        (call $create_data (i64.const 2) (i64.const 0))
        (drop (call $remote_send (local.get $parent_node) (local.get $parent)))
        (drop (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))))
