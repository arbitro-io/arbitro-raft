use criterion::{criterion_group, criterion_main, Criterion, black_box};
use tokio::runtime::Builder;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn get_payload() -> Vec<u8> {
    vec![0u8; 1024]
}

fn bench_tokio_scaling(c: &mut Criterion) {
    let payload = get_payload();
    let mut group = c.benchmark_group("Tokio Scaling (24 Cores vs 1 Core)");

    // --- Caso 1: Multi-Thread (24 Cores) ---
    let rt_multi = Builder::new_multi_thread().worker_threads(24).enable_all().build().unwrap();
    let (mut stream_multi, _) = rt_multi.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else { break; };
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 { break; }
                        let _ = socket.write_all(&buf[..n]).await;
                    }
                });
            }
        });
        let s = TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        (s, addr)
    });

    group.bench_function("multi_thread_24_cores", |b| {
        let mut recv_buf = vec![0u8; 1200];
        b.iter(|| {
            rt_multi.block_on(async {
                let _ = stream_multi.write_all(black_box(&payload)).await;
                let _ = stream_multi.read_exact(&mut recv_buf).await;
            });
        });
    });

    // --- Caso 2: Current-Thread (1 Core) ---
    let rt_single = Builder::new_current_thread().enable_all().build().unwrap();
    let (mut stream_single, _) = rt_single.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else { break; };
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 { break; }
                        let _ = socket.write_all(&buf[..n]).await;
                    }
                });
            }
        });
        let s = TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        (s, addr)
    });

    group.bench_function("current_thread_optimized", |b| {
        let mut recv_buf = vec![0u8; 1200];
        b.iter(|| {
            rt_single.block_on(async {
                let _ = stream_single.write_all(black_box(&payload)).await;
                let _ = stream_single.read_exact(&mut recv_buf).await;
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_tokio_scaling);
criterion_main!(benches);
