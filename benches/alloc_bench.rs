use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use bytes::Bytes;
use zerocopy::{FromBytes, KnownLayout, Immutable, Ref};
use zerocopy::byteorder::little_endian::U64;

// 1. Paradigma "Eager / Allocation": Parseamos el mensaje y lo alocamos
//    en propiedades reales del Struct. (Mala práctica para ruteo rápido).
struct OwnedRaftMessage {
    term: u64,
    index: u64,
    leader_id: u64,
    payload: Vec<u8>, // <-- Provoca un Heap Allocation
}

impl OwnedRaftMessage {
    fn new_parsed(bytes: &[u8]) -> Self {
        let mut term_arr = [0u8; 8];
        term_arr.copy_from_slice(&bytes[0..8]);
        
        let mut index_arr = [0u8; 8];
        index_arr.copy_from_slice(&bytes[8..16]);
        
        let mut leader_arr = [0u8; 8];
        leader_arr.copy_from_slice(&bytes[16..24]);
        
        Self {
            term: u64::from_le_bytes(term_arr),
            index: u64::from_le_bytes(index_arr),
            leader_id: u64::from_le_bytes(leader_arr),
            payload: bytes[24..].to_vec(), // Costosa copia a memoria Heap
        }
    }
}


// 2. Paradigma "Zero-Copy View": Solo guardamos un puntero al array de bytes
//    y los métodos acceden a su posición (Lo que promueve AGENTS.md).
struct ZeroCopyRaftView<'a> {
    bytes: &'a [u8],
}

impl<'a> ZeroCopyRaftView<'a> {
    fn new_view(bytes: &'a [u8]) -> Self {
        Self { bytes } // Asignación de costo cero
    }

    #[inline]
    fn term(&self) -> u64 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.bytes[0..8]);
        u64::from_le_bytes(arr)
    }

    #[inline]
    fn index(&self) -> u64 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.bytes[8..16]);
        u64::from_le_bytes(arr)
    }

    #[inline]
    fn leader_id(&self) -> u64 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.bytes[16..24]);
        u64::from_le_bytes(arr)
    }

    #[inline]
    fn payload(&self) -> &'a [u8] {
        &self.bytes[24..] // Retorna un slice, NO una copia entera
    }
}


// 3. Paradigma "Bytes": Usamos la librería bytes::Bytes (Arc-backed)
struct BytesRaftView {
    bytes: Bytes,
}

impl BytesRaftView {
    fn new_view(bytes: Bytes) -> Self {
        Self { bytes } // Costo de clonar un Arc 
    }

    #[inline]
    fn term(&self) -> u64 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.bytes[0..8]);
        u64::from_le_bytes(arr)
    }

    #[inline]
    fn payload(&self) -> Bytes {
        self.bytes.slice(24..) // Costo de slice(Arc increment)
    }
}


// 4. Paradigma "Zerocopy": El estándar de oro absoluto
#[derive(FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
struct ZerocopyHeader {
    term: U64,
    index: U64,
    leader_id: U64,
}


fn bench_allocation_vs_view(c: &mut Criterion) {
    let mut group = c.benchmark_group("Owned vs View (Zero-Copy)");
    
    // Simulamos un paquete RAFT entrante (1KB)
    let fake_network_packet = vec![1u8; 1024];

    group.bench_function("parseo_y_alocacion_eager", |b| {
        b.iter(|| {
            // Evaluando cuánto cuesta crear un struct llenando todas 
            // sus propiedades físicas reales, desencadenando allocations.
            let msg = OwnedRaftMessage::new_parsed(black_box(&fake_network_packet));
            black_box(msg.term);
            black_box(msg.payload.len());
        })
    });

    group.bench_function("construccion_view_y_lazy_getters", |b| {
        b.iter(|| {
            // Evaluando cuánto cuesta solo guardar el puntero en el View
            // y extraer únicamente el term o el slice del payload cuando
            // sea explícitamente requerido.
            let view = ZeroCopyRaftView::new_view(black_box(&fake_network_packet));
            black_box(view.term());
            black_box(view.payload().len());
        })
    });

    let bytes_packet = Bytes::from(vec![1u8; 1024]);
    group.bench_function("construccion_view_con_bytes", |b| {
        b.iter(|| {
            // Evaluando cuánto cuesta estructurarlo pasando Bytes (que por debajo clona su Arc),
            // y hacer slices reteniendo el Arc.
            let view = BytesRaftView::new_view(black_box(bytes_packet.clone()));
            black_box(view.term());
            black_box(view.payload().len());
        })
    });

    group.bench_function("construccion_view_zerocopy", |b| {
        b.iter(|| {
            // Evaluando cuánto cuesta castear vía zerocopy un prefijo y referenciar payload.
            let bytes = black_box(&fake_network_packet[..]);
            let (header_ref, payload_slice) = Ref::<_, ZerocopyHeader>::from_prefix(bytes).unwrap();
            black_box(header_ref.term.get());
            black_box(payload_slice.len());
        })
    });

    group.finish();

    let mut getters_group = c.benchmark_group("Getters Only (Construccion excluida)");
    
    let eager_msg = OwnedRaftMessage::new_parsed(&fake_network_packet);
    getters_group.bench_function("getter_eager", |b| {
        b.iter(|| {
            black_box(eager_msg.term);
            black_box(eager_msg.payload.len());
        })
    });

    let view_u8 = ZeroCopyRaftView::new_view(&fake_network_packet);
    getters_group.bench_function("getter_view_u8", |b| {
        b.iter(|| {
            black_box(view_u8.term());
            black_box(view_u8.payload().len());
        })
    });

    let view_bytes = BytesRaftView::new_view(bytes_packet.clone());
    getters_group.bench_function("getter_view_bytes", |b| {
        b.iter(|| {
            black_box(view_bytes.term());
            black_box(view_bytes.payload().len());
        })
    });

    let (zero_header_ref, zero_payload_slice) = Ref::<_, ZerocopyHeader>::from_prefix(&fake_network_packet[..]).unwrap();
    getters_group.bench_function("getter_view_zerocopy", |b| {
        b.iter(|| {
            // Evaluamos solo accesar al campo extraído y medimos el len del slice pre-extraído
            black_box(zero_header_ref.term.get());
            black_box(zero_payload_slice.len());
        })
    });

    getters_group.finish();
}

criterion_group!(benches, bench_allocation_vs_view);
criterion_main!(benches);
