# ARBITRO-RAFT — GUÍA DE CONTRIBUCIÓN

Este crate implementa el núcleo del protocolo Raft para el ecosistema Arbitro.
Hereda **todas** las reglas de `AGENTS.md` del repositorio raíz. Las reglas
de este documento son de alcance crate y refinan o especifican lo que `AGENTS.md`
establece a nivel ecosistema.

---

## CAPAS DEL CRATE

El crate tiene tres capas ordenadas. Una capa superior **nunca** importa de una
capa igual o superior a ella en otra rama.

```
protocol/          ← Serialización wire. Sin lógica de Raft.
state/ types/ error/ entry/ config/ validation/ traits/  ← Tipos puros.
api/               ← Lógica Raft + orquestación de las capas inferiores.
```

| Módulo | Responsabilidad |
|---|---|
| `protocol/codec.rs` | Serialización / deserialización zerocopy de frames |
| `protocol/view.rs` | Vistas zero-copy sobre frames recibidos |
| `protocol/message.rs` | Tipos owned para frames creados en sender |
| `dispatch/` | Protocolo de dispatch personalizado sobre Raft custom |
| `api/node/` | Máquina de estados Raft (election, replication, snapshot, dispatch) |
| `api/arbitro_raft.rs` | Loop de ejecución, batching, scheduling de timers |
| `api/custom_registry.rs` | Registro de handlers de dispatch por command byte |

---

## ESTADO RAFT — QUÉ ES HARD Y QUÉ ES SOFT

Esta distinción es un invariante de correctness, no de estilo.

### HardState — persiste en cada transición

```
current_term   ← obligatorio en disco antes de cualquier send
voted_for      ← obligatorio en disco antes de votar
```

> **`commit_index` NO pertenece al HardState.**
> Es estado volátil. Debe vivir en `SoftState` o en memoria directa del nodo.
> Restaurar `commit_index` desde disco puede hacer creer al nodo que entradas
> están comprometidas cuando el log fue truncado — violación de linearizabilidad.

### SoftState — reconstruido en memoria al reiniciar

```
role          (Follower al iniciar siempre)
leader_id     (None al iniciar siempre)
is_leader     (false al iniciar siempre)
commit_index  (0 al iniciar; el líder lo propaga via AppendEntries)
```

---

## PROTOCOLO WIRE — REGLAS DE CODIFICACIÓN

### Constantes

Todas las constantes de protocolo están definidas en `protocol/codec.rs`.
**Nunca** uses literales hex directamente — usa las constantes nombradas.

| Constante | Valor | Uso |
|---|---|---|
| `RAFT_MAGIC` | `0x5241_4654` | Identifica frame Raft |
| `RAFT_VERSION` | `0x01` | Versión de protocolo |
| `RAFT_FRAME_HEADER_SIZE` | `size_of::<RaftFrameHeader>()` | Offset al body |
| `RAFT_DISPATCH_MAGIC` | `0x4453_5054` | Frame de dispatch custom |
| `RAFT_DISPATCH_RESPONSE_MAGIC` | `0x4452_5350` | Frame de respuesta dispatch |

### Endianness

Todo el wire es **little-endian**. Usar `zerocopy::byteorder::little_endian::{U16, U32, U64}`
para todos los campos multi-byte en structs `#[repr(C)]`.

### Padding

Cada struct de wire debe estar alineado a **8 bytes**. Los campos `_pad` explícitos
son parte del contrato de formato — no eliminar, no cambiar de tamaño sin bump de versión.

### Validación al recibir

1. Verificar magic → error si no coincide.
2. Verificar version → error si no soportada.
3. Verificar que `body_len` coincide con bytes restantes → error si no.
4. Para AppendEntries: iterar todos los `EntryHeader` y verificar que `payload_end <= body.len()`.

**Nunca** accedas a un campo de un frame sin haberlo validado primero. Las funciones
`parse_*_view` son la única entrada válida a frames recibidos.

---

## HOT PATH — ESPECIFICACIONES PARA ESTE CRATE

Complementa las reglas globales de `AGENTS.md` con lo siguiente:

### Zero-allocation en el hot path

Los `Vec` de scratchpad pre-alocados en `RaftNode` son la única forma válida
de acumular datos temporales en funciones hot:

```rust
// ✅ Correcto — reusar scratchpad pre-alocado
self.scratch_entries.clear();
self.storage.read_entries(from, to, &mut self.scratch_entries)?;

// ❌ Prohibido — new allocation por llamada
let entries: Vec<LogEntry> = self.storage.read_entries_owned(from, to)?;
```

Los campos de scratchpad en `RaftNode` son:
- `scratch_entries: Vec<LogEntry>` — entries temporales para replicación
- `scratch_indexes: Vec<LogIndex>` — índices de un batch propuesto
- `scratch_peers: Vec<PeerId>` — peers destino temporales
- `scratch_pending: HashMap<PeerId, AppendAttemptState>` — estado de intento en curso
- `scratch_started: HashMap<PeerId, Instant>` — timing de operaciones en curso

**Siempre** llamar `.clear()` antes de usar un scratchpad, nunca asumir que está vacío.

### Views vs Owned en el hot path

| Situación | Usar |
|---|---|
| Frame recibido — procesado sin salir de la función | `*View` (zero-copy) |
| Frame que debe ser almacenado o enviado por canal | `Owned` (via `.to_owned()`) |
| Bytes que se re-envían sin modificar | `Bytes::clone()` (Arc bump, no copia) |

`Bytes::clone()` es O(1) y no copia el buffer — es la forma correcta de pasar
el mismo frame a múltiples destinatarios.

### Dispatch en el hot path — switch, no if-chain

```rust
// ✅ handle_inbound — O(1) dispatch
match inbound.message {
    RaftMessageView::RequestVote(msg)         => self.handle_request_vote(msg).await,
    RaftMessageView::RequestVoteResp(msg)     => self.handle_request_vote_response(msg).await,
    RaftMessageView::AppendEntries(msg)       => self.handle_append_entries(msg).await,
    RaftMessageView::AppendEntriesResp(msg)  => self.handle_append_entries_response(msg).await,
    RaftMessageView::InstallSnapshot(msg)     => self.handle_install_snapshot(msg).await,
    RaftMessageView::InstallSnapshotResp(msg)=> self.handle_install_snapshot_response(msg).await,
    RaftMessageView::Custom(msg)             => self.handle_custom_message(msg).await,
    RaftMessageView::CustomResponse(msg)     => self.handle_custom_response(msg).await,
    // No `_ =>` implícito — añadir variant aquí si se añade al enum
}
```

---

## CORRECTNESS RAFT — INVARIANTES NO NEGOCIABLES

### Orden de persistencia antes de enviar

Antes de enviar cualquier mensaje a la red, las operaciones de disco deben
haberse completado en este orden:

```
1. Si term cambió → save_hard_state (con nuevo term y voted_for = None)
2. Si voted_for cambió → save_hard_state
3. Si se appendaron entries → append_entries (fsync si el storage lo soporta)
4. ENTONCES → transport.send(...)
```

Invertir este orden puede causar que un nodo vote dos veces en el mismo term
tras un crash, violando la unicidad del líder.

### Quórum

```rust
pub(crate) fn quorum(nodes: usize) -> usize { (nodes / 2) + 1 }
```

Esta función es la única fuente de verdad para quórum. No calcular `(n/2)+1`
inline en ningún otro lugar.

### Election loop — condición de terminación

El loop de recolección de votos debe iterar hasta que:
- Se alcance `votes >= votes_needed`, **o**
- Todos los posibles respondedores hayan contestado (`responders.len() >= possible_votes`), **o**
- Expire el timeout.

```rust
// ✅ Correcto
while votes < votes_needed && responders.len() < possible_votes {
    ...
}

// ❌ Incorrecto — termina un respondedor antes, puede perder el voto decisivo
while votes < votes_needed && responders.len() < possible_votes.saturating_sub(1) {
    ...
}
```

### Replicación — nunca silenciar errores de red

Cuando el líder envía AppendEntries a peers, un fallo de transporte no es
un error fatal, pero **debe** ser registrado en el estado de progreso del peer.
Nunca usar `let _ = transport.send(...)` en funciones que contribuyen al quórum
de replicación.

```rust
// ✅ Correcto — best-effort, el fallo se propaga al caller como bool
async fn send_best_effort(&self, peer: PeerId, msg: RaftMessage, _phase: &str) -> bool {
    self.transport.send(peer, msg).await.is_ok()
}

// ❌ Prohibido en funciones que esperan quórum
let _ = self.send_append_attempt(peer, 1).await;
```

### Snapshot — integridad de offset

Un follower que recibe un chunk de snapshot verifica:
`pending.bytes.len() as u64 == msg.offset()`

Si no coincide, responde con `accepted: false` y `next_offset = pending.bytes.len()`.
El líder debe respetar este `next_offset` y reenviar desde ahí.
**Nunca** asumir que los chunks llegan en orden o sin gaps.

---

## SISTEMA DE DISPATCH — REGLAS DE USO

### Registro de handlers

- Cada `command: u8` puede tener **exactamente un** handler registrado.
- Intentar registrar un segundo handler para el mismo command es `Err`.
- Los handlers son registrados en `RaftCustomRegistry` antes de iniciar el loop.
- **No** registrar handlers desde dentro del loop de ejecución.

### DispatchSpec — fuente de verdad de encode/decode

Cada comando de dispatch tiene un `DispatchSpec<P, R>` que contiene los
codecs de parámetros y respuesta. Este spec es la única fuente de verdad.

```rust
// ✅ Usar spec para encode y decode
let body = spec.encode_params(&params)?;
let response = spec.decode_response(bytes)?;

// ❌ Nunca encode/decode inline sin pasar por spec
let body = serde_json::to_vec(&params)?; // JSON prohibido en hot path además
```

### Scope de dispatch

```
DispatchScope::All       → todos los nodos (incluyendo self)
DispatchScope::Others    → todos excepto self
DispatchScope::Followers → solo followers
DispatchScope::Leader    → solo el líder conocido
DispatchScope::LocalOnly → solo self (sin red)
```

El scope es parte del frame wire — no puede cambiarse después del `build()`.

### Policy de ACK y fallo

- `DispatchAckPolicy::Quorum` es el default — requiere `(active/2)+1` aceptaciones.
- `DispatchFailPolicy::AllowFailures` es el default — permite cualquier número de fallos.
- Un `DispatchHandle` está listo cuando `completion.is_some()`.
- Nunca esperar un handle con `.wait()` dentro del loop principal del nodo —
  usar `.try_result()` para polling no bloqueante.

---

## TRAITS — CONTRATO DE IMPLEMENTACIÓN

### `RaftStorage`

| Método | Semántica |
|---|---|
| `load_hard_state` | Llamado una vez al init. Debe ser idempotente. |
| `save_hard_state` | Debe ser síncrono y durable antes de retornar. |
| `append_entries` | Los entries deben ser durables antes de retornar. |
| `read_entries(from, to, out)` | Rango `[from, to)`. `out` se **extiende**, no se limpia. |
| `truncate_suffix(from)` | Elimina `[from, ∞)`. Durable antes de retornar. |
| `last_log_position` | **DEBE** sobrescribirse — el default hace O(N) full scan. |
| `entry_at` | **DEBE** sobrescribirse — el default aloca un Vec por llamada. |
| `save_snapshot` | Atómica — o el snapshot completo o nada. |

### `RaftTransport`

| Método | Semántica |
|---|---|
| `send` | Best-effort. El error no debe ser fatal para el nodo. |
| `recv` | Bloqueante hasta recibir un frame válido. |
| `recv_timeout(d)` | `None` si expiró el timeout, `Some` si llegó frame. |

`recv` y `recv_timeout` deben retornar frames **ya parseados** como
`InboundRaftMessageView`. La codificación/decodificación ocurre en el transport,
no en el nodo.

### `StateMachine`

El trait existe para extensiones futuras. No tiene métodos en v0.1.
La aplicación del log al estado de la máquina es responsabilidad del usuario
del crate, no de `arbitro-raft` en v0.1.

---

## OBSERVABILIDAD — TRACING Y MÉTRICAS

### Qué usar

```rust
// ✅ Para eventos de management path (election ganada, snapshot completado)
tracing::info!(node_id = ..., term = ..., "leader elected");

// ✅ Para diagnóstico de replicación (solo en funciones non-hot)
tracing::debug!(voter = ..., votes, needed = ..., "vote granted");

// ✅ Para tracing condicional muy verbose (controlado por env var)
if super::trace_enabled() {
    tracing::trace!(...);
}

// ❌ PROHIBIDO en cualquier contexto de producción
eprintln!(...);
println!(...);
```

### La variable `ARBITRO_RAFT_TRACE`

`trace_enabled()` (en `node/mod.rs`) y la función homónima en `protocol/codec.rs`
leen `ARBITRO_RAFT_TRACE` una sola vez via `OnceLock`. El output de trace
**nunca** puede usar `eprintln!` — debe usar `tracing::trace!` o bien
`tracing::event!(Level::TRACE, ...)`.

Toda instrumentación de timing (`Instant::now()`) solo puede existir dentro
de un bloque `if trace_enabled()`. No medir en el hot path incondicional.

---

## ESTRUCTURA DE ARCHIVOS — LÍMITES

| Ámbito | Límite |
|---|---|
| Archivo `.rs` | 400 líneas |
| Función / método | 60 líneas |
| `impl` block | 200 líneas |

Cuando un archivo (`replication.rs`, `codec.rs`) supere las 400 líneas,
extraer en submódulos con responsabilidad única:
- `replication/heartbeat.rs`, `replication/propose.rs`, `replication/handler.rs`
- `codec/encode.rs`, `codec/decode.rs`, `codec/validate.rs`

---

## NOMBRADO DE SUFIJOS Y PREFIJOS EN ESTE CRATE

Complementa las convenciones globales de `AGENTS.md`:

| Patrón | Ejemplo | Significado |
|---|---|---|
| `*View` | `AppendEntriesView` | Tipo que lee desde `Bytes` sin copiar |
| `*Resp` | `AppendEntriesResp` | Mensaje de respuesta (owned) |
| `*RespView` | `AppendEntriesRespView` | Vista de mensaje de respuesta |
| `handle_*` | `handle_append_entries` | Handler de frame inbound |
| `build_*` | `build_append_for_peer` | Construye un mensaje outbound |
| `send_*_once` | `send_heartbeat_once` | Envío único, sin loop interno |
| `*_once` | `propose_once`, `campaign_once` | Una iteración, sin retry loop externo |
| `scratch_*` | `scratch_entries` | Buffer pre-alocado de scratchpad en `RaftNode` |
| `pending_*` | `pending_custom`, `pending_snapshots` | Estado en vuelo esperando respuesta |

---

## LO QUE UN PR NO PUEDE HACER (CHECKLIST)

Antes de proponer cualquier cambio, verificar:

- [ ] No `eprintln!` / `println!` en ningún archivo de producción
- [ ] No `format!` fuera de bloques de error o tracing — nunca inline en el hot path
- [ ] No `Instant::now()` fuera de bloques `if trace_enabled()`
- [ ] No `Vec::new()` / `HashMap::new()` en funciones llamadas por frame (usar scratchpad)
- [ ] `commit_index` no está en `HardState`
- [ ] El election loop termina en `responders.len() < possible_votes` (sin `- 1`)
- [ ] `replicate_batch_async` y similares no silencian errores de replicación con `let _`
- [ ] `save_hard_state` se llama **antes** de `transport.send` en cualquier transición de term
- [ ] Toda nueva constante de protocolo está en `protocol/codec.rs` o `dispatch/view.rs`
- [ ] No hay literales hex de protocolo fuera de los archivos de constantes
- [ ] `RaftStorage` implementada por tests/benchmarks sobreescribe `last_log_position` y `entry_at`
- [ ] No hay `match` con `_ => {}` silencioso en dispatch de frames — el default debe ser error
- [ ] Archivos no superan 400 líneas
