---
trigger: always_on
---

# PERFORMANCE, ZERO-COPY & HARDWARE SYMPATHY — ABSOLUTE LAWS

> Si una sola regla se rompe, el sistema está mal diseñado.
> No hay excepciones. No hay conveniencia. No hay “solo esta vez”.

## 1) PERFORMANCE IS THE PRIMARY REQUIREMENT

1. **El rendimiento manda sobre la ergonomía, estética y conveniencia del código.**
2. **Toda decisión debe justificarse por costo real de CPU, memoria, caché, locks o syscalls.**
3. **Si una abstracción oculta costo, esa abstracción no puede existir en hot path.**
4. **Toda operación en hot path debe ser predecible, medible y constante o amortizada.**
5. **Si algo introduce latencia evitable, debe eliminarse o rediseñarse.**

## 2) ZERO-COPY IS MANDATORY

6. **Prohibido copiar datos en hot path.**
7. **Los datos deben viajar como slices, views o referencias sobre memoria existente.**
8. **Parsing eager está prohibido; solo se decodifica lo que se usa y cuando se usa.**
9. **Los tipos de wire deben ser los mismos tipos usados por el engine cuando sea posible.**
10. **Toda serialización o transformación intermedia debe considerarse una violación hasta probar lo contrario.**

## 3) ALLOCATION IS A BUG

11. **Cero allocations en hot path.**
12. **Cero growth dinámico en steady state.**
13. **Todo buffer, scratch, batch y estructura temporal debe pre-alocarse.**
14. **Todo buffer reusable debe limpiarse con `.clear()`; nunca recrearse.**
15. **Capacidad monotónica: crece si es necesario, nunca se reduce en runtime.**
16. **Crear `String`, `Vec`, `Bytes`, `Box`, `Arc` o equivalentes en hot path está prohibido salvo prueba explícita de costo aceptable y aprobación de diseño.**
17. **Toda ownership conversion innecesaria es una violación de performance.**

## 4) CACHE AND MEMORY LAYOUT ARE LAW

18. **Toda estructura hot debe diseñarse para caché, no para comodidad.**
19. **Hot structs deben ser pequeñas, contiguas y con layout explícito.**
20. **`#[repr(C)]` es obligatorio donde el layout importe.**
21. **Todo dato mutado por hilos distintos debe aislarse para evitar false sharing.**
22. **Pointer chasing en hot path está prohibido.**
23. **Se prefieren arrays, slices y buffers contiguos sobre estructuras enlazadas o dispersas.**
24. **Si una estructura empeora locality, debe rediseñarse.**
25. **Menos bytes en hot data siempre gana.**

## 5) BRANCHES, LOCKS AND SYSCALLS MUST BE MINIMIZED

26. **Toda bifurcación evitable es una penalización y debe eliminarse.**
27. **Debe existir un solo camino hot siempre que sea posible.**
28. **Single-message debe tratarse como batch de 1.**
29. **Los casos más frecuentes deben ejecutarse primero.**
30. **Todo lock debe ser corto, localizado y jamás compartido innecesariamente.**
31. **La contención entre streams, shards o subjects está prohibida por diseño.**
32. **Un syscall por mensaje es una violación; todo I/O debe batchearse.**
33. **`write_vectored` o equivalente debe preferirse sobre writes unitarios.**
34. **No se permite logging, tracing ni formatting en hot path.**
35. **No se permite pedir tiempo al sistema en hot path salvo que sea estrictamente obligatorio.**

## 6) HOT PATH MUST BE MECHANICAL

36. **El hot path debe ser simple, lineal y repetible.**
37. **Nada de callbacks complejos, capas decorativas o pipelines abstractos en zonas críticas.**
38. **Toda función hot debe ser pequeña, inlineable y explícita.**
39. **Toda operación hot debe trabajar sobre bytes, offsets, índices o views, no sobre objetos ricos.**
40. **Toda validación pesada, conversión textual o enriquecimiento pertenece al cold path.**

## 7) OWNERSHIP, TYPES AND WIRE FORMAT

41. **Los tipos de red deben ser triviales, estables y binarios.**
42. **Field-by-field copy está prohibido cuando pueda usarse layout directo.**
43. **No se permiten tipos espejo `Owned` si el tipo base ya puede representar la data eficientemente.**
44. **Las respuestas deben construirse con headers en stack y body por referencia.**
45. **Toda respuesta hot debe ser zero-alloc.**

## 8) CONCURRENCY MUST RESPECT THE MACHINE

46. **La concurrencia no debe aumentar trabajo inútil del scheduler, caché o memoria.**
47. **Cada shard, stream o lane debe aislar su estado mutable.**
48. **No se permite compartir estructuras centrales si eso introduce invalidación de caché o contención.**
49. **La distribución del trabajo debe favorecer locality, batching y predictibilidad.**
50. **Mover trabajo entre hilos sin necesidad es una violación.**

## 9) NO HIDDEN COSTS

51. **Toda operación aparentemente simple debe evaluarse por su costo real.**
52. **APIs cómodas que copian, asignan, formatean o bloquean están prohibidas en hot path.**
53. **Toda dependencia que no respete estas reglas queda fuera del núcleo crítico.**
54. **Si una librería obliga a copiar o alocar, no entra al core.**
55. **No se aceptan “helpers” que oculten heap, clones o conversiones implícitas.**

## 10) ENFORCEMENT

56. **Toda violación de estas reglas debe tratarse como bug de arquitectura, no como detalle menor.**
57. **Toda excepción debe demostrar con benchmark y profiling que no degrada steady-state ni tail latency.**
58. **Sin benchmark, la excepción no existe.**
59. **Sin evidencia, toda alloc, copia, lock extra o syscall extra se considera incorrecta.**
60. **Si hay duda entre diseño cómodo y diseño rápido, siempre gana el rápido.**

## SUPREME RULE

**El sistema no se diseña para que “funcione”.**
**Se diseña para respetar CPU, caché, memoria y kernel en todo momento.**
**Todo lo demás es secundario.**