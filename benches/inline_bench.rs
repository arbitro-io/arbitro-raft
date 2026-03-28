use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

// Escenario 1: Función libre (Pura) iterando sobre los bytes con offsets fijos
#[inline]
fn get_term_id_pure(bytes: &[u8]) -> u64 {
    // Simulamos un offset harcodeado en la posicion 8 de tamaño 8 bytes
    let start = 8;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[start..start + 8]);
    u64::from_le_bytes(arr)
}

// Escenario 2: Objeto inline donde delegamos en métodos de la View
struct FrameView<'a> {
    bytes: &'a [u8],
}

impl<'a> FrameView<'a> {
    #[inline]
    pub fn term_id(&self) -> u64 {
        let start = 8;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.bytes[start..start + 8]);
        u64::from_le_bytes(arr)
    }
}

// Escenario 3: Qué pasa si NO ponemos #[inline]?
struct FrameViewNoInline<'a> {
    bytes: &'a [u8],
}

impl<'a> FrameViewNoInline<'a> {
    // Simulamos sin inline (A veces cargo igual hace inline transparentemente,
    // pero evitamos forzarlo)
    #[inline(never)]
    pub fn term_id(&self) -> u64 {
        let start = 8;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.bytes[start..start + 8]);
        u64::from_le_bytes(arr)
    }
}

fn bench_inline_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("Inline vs Pure vs NoInline");

    // Un frame simulado
    let bytes: Bytes = Bytes::from(vec![0u8; 64]);
    let slice = bytes.as_ref();

    group.bench_function("function_pure_inline", |b| {
        b.iter(|| {
            let term = get_term_id_pure(black_box(slice));
            black_box(term);
        })
    });

    group.bench_function("method_struct_inline", |b| {
        b.iter(|| {
            let view = FrameView { bytes: slice };
            let term = view.term_id();
            black_box(term);
        })
    });

    group.bench_function("method_struct_no_inline", |b| {
        b.iter(|| {
            let view = FrameViewNoInline { bytes: slice };
            let term = view.term_id();
            black_box(term);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_inline_comparison);
criterion_main!(benches);
