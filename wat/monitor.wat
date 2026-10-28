;; Run with `lunatic node --wasm`: monitors processes on another node. Prints "monitor ok" once
;; - the monitor of a process fired after the 100 messages the process sent before exiting,
;; - the monitor of a process that already finished fired,
;; - and the monitor of a process that never existed fired,
;; and "monitor failed" otherwise.
(module
    (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
    (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
    (import "lunatic::distributed" "node_id" (func $node_id (result i64)))
    (import "lunatic::distributed" "get_nodes" (func $get_nodes (param i32 i32) (result i32)))
    (import "lunatic::distributed" "module_id" (func $module_id (result i64)))
    (import "lunatic::distributed" "spawn"
        (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "send" (func $send (param i64 i64) (result i32)))
    (import "lunatic::distributed" "monitor" (func $monitor (param i64 i64)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))

    (memory (export "memory") 1)
    ;; 0..12: fd_write's iovec and written length.
    (data (i32.const 16) "sender")
    (data (i32.const 24) "pinger")
    (data (i32.const 32) "monitor ok\n")
    (data (i32.const 64) "monitor failed\n")
    ;; 128..162: the parameters of the spawned functions. 192: the spawned process ID.
    ;; 256..384: node IDs.

    (func $print (param $ptr i32) (param $len i32)
        (i32.store (i32.const 0) (local.get $ptr))
        (i32.store (i32.const 4) (local.get $len))
        (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8))))

    (func $fail
        (call $print (i32.const 64) (i32.const 15))
        (call $proc_exit (i32.const 1)))

    ;; Writes an i64 spawn parameter: its type (0x7E) followed by its value as a u128.
    (func $param (param $ptr i32) (param $value i64)
        (i32.store8 (local.get $ptr) (i32.const 0x7E))
        (i64.store (i32.add (local.get $ptr) (i32.const 1)) (local.get $value))
        (i64.store (i32.add (local.get $ptr) (i32.const 9)) (i64.const 0)))

    ;; Spawns the function named by `name_ptr` (6 bytes) on `node`, returning its process ID.
    (func $spawn_on (param $node i64) (param $name_ptr i32) (result i64)
        (if (call $spawn (local.get $node) (i64.const -1) (call $module_id)
                (local.get $name_ptr) (i32.const 6) (i32.const 128) (i32.const 34) (i32.const 192))
            (then (call $fail)))
        (i64.load (i32.const 192)))

    ;; Receives the next message: 0 for data, 2 for a monitored process' death.
    (func $next (result i32)
        (call $receive (i32.const 0) (i32.const 0) (i64.const 10000)))

    (func (export "_start")
        (local $count i32)
        (local $i i32)
        (local $other i64)
        (local $process i64)

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
        ;; The spawned functions message this process.
        (call $param (i32.const 128) (call $node_id))
        (call $param (i32.const 145) (call $process_id))

        ;; The monitor fires after the messages the process sent.
        (local.set $process (call $spawn_on (local.get $other) (i32.const 16)))
        (call $monitor (local.get $other) (local.get $process))
        (call $create_data (i64.const 0) (i64.const 0))
        (if (call $send (local.get $other) (local.get $process)) (then (call $fail)))
        (local.set $count (i32.const 0))
        (block $died
            (loop $messages
                (br_if $died (i32.eq (call $next) (i32.const 2)))
                (local.set $count (i32.add (local.get $count) (i32.const 1)))
                (br $messages)))
        (if (i32.ne (local.get $count) (i32.const 100)) (then (call $fail)))

        ;; The monitor of a process that already finished fires.
        (local.set $process (call $spawn_on (local.get $other) (i32.const 24)))
        (if (call $next) (then (call $fail)))
        (call $sleep_ms (i64.const 200))
        (call $monitor (local.get $other) (local.get $process))
        (if (i32.ne (call $next) (i32.const 2)) (then (call $fail)))

        ;; The monitor of a process that never existed fires.
        (call $monitor (local.get $other) (i64.const 1000000))
        (if (i32.ne (call $next) (i32.const 2)) (then (call $fail)))

        (call $print (i32.const 32) (i32.const 11)))

    ;; Waits for a message, then sends 100 messages to the parent and exits.
    (func (export "sender") (param $parent_node i64) (param $parent i64)
        (local $i i32)
        (if (call $next) (then (call $fail)))
        (loop $messages
            (call $create_data (i64.const 0) (i64.const 0))
            (drop (call $send (local.get $parent_node) (local.get $parent)))
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $messages (i32.lt_u (local.get $i) (i32.const 100)))))

    ;; Sends a message to the parent and exits.
    (func (export "pinger") (param $parent_node i64) (param $parent i64)
        (call $create_data (i64.const 0) (i64.const 0))
        (drop (call $send (local.get $parent_node) (local.get $parent)))))
