use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use bytes::{Bytes, BytesMut, BufMut};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref};
use zerocopy::byteorder::little_endian::U64;

// --- Infraestructura Zerocopy ---
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes, Clone, Copy, Debug)]
#[repr(C)]
struct ZerocopyHeader {
    term: U64,
    index: U64,
    leader_id: U64,
}

// --- Escenario 1: Manual u8 ---
struct U8View<'a> {
    bytes: &'a [u8],
}
impl<'a> U8View<'a> {
    #[inline]
    fn term(&self) -> u64 {
        u64::from_le_bytes(self.bytes[0..8].try_into().unwrap())
    }
}

// --- Escenario 2: Bytes Lib ---
struct BytesView {
    bytes: Bytes,
}
impl BytesView {
    #[inline]
    fn term(&self) -> u64 {
        u64::from_le_bytes(self.bytes[0..8].try_into().unwrap())
    }
}

// --- Funciones de ensamble ---
#[inline]
fn assemble_u8(term: u64, index: u64, leader_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(24 + payload.len());
    v.extend_from_slice(&term.to_le_bytes());
    v.extend_from_slice(&index.to_le_bytes());
    v.extend_from_slice(&leader_id.to_le_bytes());
    v.extend_from_slice(payload);
    v
}

#[inline]
fn assemble_bytes(term: u64, index: u64, leader_id: u64, payload: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(24 + payload.len());
    b.put_u64_le(term);
    b.put_u64_le(index);
    b.put_u64_le(leader_id);
    b.put_slice(payload);
    b.freeze()
}

#[inline]
fn assemble_zerocopy(term: u64, index: u64, leader_id: u64, payload: &[u8]) -> Vec<u8> {
    let header = ZerocopyHeader {
        term: U64::new(term),
        index: U64::new(index),
        leader_id: U64::new(leader_id),
    };
    let mut v = Vec::with_capacity(24 + payload.len());
    v.extend_from_slice(header.as_bytes());
    v.extend_from_slice(payload);
    v
}

// --- Servidor de Eco ---
fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue; };
            let mut buf = [0u8; 2048];
            while let Ok(n) = stream.read(&mut buf) {
                if n == 0 { break; }
                if stream.write_all(&buf[..n]).is_err() { break; }
            }
        }
    });
    
    addr
}

fn bench_tcp_full_lifecycle(c: &mut Criterion) {
    let addr = start_echo_server();
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream.set_nodelay(true).unwrap();
    
    let payload = vec![0u8; 1024];
    let mut recv_buf = vec![0u8; 24 + 1024];
    
    let mut group = c.benchmark_group("TCP Full Lifecycle Sync (Assemble+Send+Recv+Construct)");
    
    group.bench_function("u8_manual", |b| {
        b.iter(|| {
            let data = assemble_u8(black_box(1), black_box(1), black_box(1), black_box(&payload));
            stream.write_all(&data).unwrap();
            stream.read_exact(&mut recv_buf).unwrap();
            let view = U8View { bytes: &recv_buf };
            black_box(view.term());
        })
    });

    group.bench_function("bytes_lib", |b| {
        b.iter(|| {
            let data = assemble_bytes(black_box(1), black_box(1), black_box(1), black_box(&payload));
            stream.write_all(&data).unwrap();
            stream.read_exact(&mut recv_buf).unwrap();
            let view = BytesView { bytes: Bytes::copy_from_slice(&recv_buf) };
            black_box(view.term());
        })
    });

    group.bench_function("zerocopy_ref", |b| {
        b.iter(|| {
            let data = assemble_zerocopy(black_box(1), black_box(1), black_box(1), black_box(&payload));
            stream.write_all(&data).unwrap();
            stream.read_exact(&mut recv_buf).unwrap();
            let (header_ref, _) = Ref::<_, ZerocopyHeader>::from_prefix(&recv_buf[..]).unwrap();
            black_box(header_ref.term.get());
        })
    });

    group.bench_function("pure_functions", |b| {
        b.iter(|| {
            let data = assemble_u8(black_box(1), black_box(1), black_box(1), black_box(&payload));
            stream.write_all(&data).unwrap();
            stream.read_exact(&mut recv_buf).unwrap();
            black_box(u64::from_le_bytes(recv_buf[0..8].try_into().unwrap()));
        })
    });

    group.finish();
}

criterion_group!(benches, bench_tcp_full_lifecycle);
criterion_main!(benches);
