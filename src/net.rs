// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//! The networking module, providing TCP sockets.
//!
//! Fiona's I/O objects, such as [`TcpStream`] are simple copyable
//! handles to an underlying allocation. This is done primarily so that the
//! runtime and the kernel can split ownership of the underlying data and
//! resources. io_uring is a fully asynchronous completion-based subsystem and
//! because of this, operations scheduled through the ring are fully concurrent
//! with what's running in userspace.
//!
//! To prevent many potential issues with use-after-free  or accidental fd
//! reuse, each I/O object is heap-allocated with an internal hidden reference
//! count. `TcpStream` is spritually similar to an `Rc<T>`.
//!
//! Because `TcpStream` and `TcpListener` both support cloning, they can be
//! trivially transferred across task boundaries and because Fiona is a purely
//! single-threaded runtime, this is done without any synchronization overhead.
//! This enables users to write natural-looking code without dealing with
//! lifetimes.

pub(crate) mod tcp;

pub use tcp::*;
