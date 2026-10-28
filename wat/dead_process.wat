;; Run with `lunatic node --wasm`: spawns `worker`, which exits right away, on another node. A
;; request to the dead worker then reports that the process doesn't exist (1) instead of waiting for
;; a reply that never comes, and messages to it are dropped. Prints "dead process ok" or
;; "dead process failed".
(module
    (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
    (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
    (import "lunatic::distributed" "node_id" (func $node_id (result i64)))
    (import "lunatic::distributed" "get_nodes" (func $get_nodes (param i32 i32) (result i32)))
    (import "lunatic::distributed" "module_id" (func $module_id (result i64)))
    (import "lunatic::distributed" "spawn"
        (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "send" (func $send (param i64 i64) (result i32)))
    (import "lunatic::distributed" "send_receive_skip_search"
        (func $send_receive_skip_search (param i64 i64 i64 i64) (result i32)))
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))

    (memory (export "memory") 1)
    ;; 0..12: fd_write's iovec and written length.
    (data (i32.const 16) "worker")
    (data (i32.const 32) "dead process ok\n")
    (data (i32.const 64) "dead process failed\n")
    ;; 192: the spawned process ID. 256..384: node IDs.

    (func $print (param $ptr i32) (param $len i32)
        (i32.store (i32.const 0) (local.get $ptr))
        (i32.store (i32.const 4) (local.get $len))
        (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8))))

    (func $fail
        (call $print (i32.const 64) (i32.const 20))
        (call $proc_exit (i32.const 1)))

    (func (export "_start")
        (local $count i32)
        (local $i i32)
        (local $other i64)
        (local $result i32)

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

        (if (call $spawn (local.get $other) (i64.const -1) (call $module_id)
                (i32.const 16) (i32.const 6) (i32.const 128) (i32.const 0) (i32.const 192))
            (then (call $fail)))

        ;; Requests that arrive before the worker exited time out (9027) instead.
        (local.set $i (i32.const 0))
        (block $dead
            (loop $request
                (if (i32.ge_u (local.get $i) (i32.const 50)) (then (call $fail)))
                (call $create_data (i64.const 0) (i64.const 0))
                (local.set $result (call $send_receive_skip_search
                    (local.get $other) (i64.load (i32.const 192)) (i64.const 1) (i64.const 100)))
                (br_if $dead (i32.eq (local.get $result) (i32.const 1)))
                (if (i32.ne (local.get $result) (i32.const 9027)) (then (call $fail)))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $request)))

        (local.set $i (i32.const 0))
        (loop $message
            (call $create_data (i64.const 0) (i64.const 0))
            (if (call $send (local.get $other) (i64.load (i32.const 192)))
                (then (call $fail)))
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $message (i32.lt_u (local.get $i) (i32.const 3))))

        (call $print (i32.const 32) (i32.const 16)))

    (func (export "worker")))
