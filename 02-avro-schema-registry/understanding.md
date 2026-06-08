# Avro + Schema Registry: Deep Understanding

## Table of Contents

1. [What is Schema Registry?](#1-what-is-schema-registry)
2. [How Serialization Works (Producer Side)](#2-how-serialization-works-producer-side)
3. [The Wire Format](#3-the-wire-format)
4. [How Deserialization Works (Consumer Side)](#4-how-deserialization-works-consumer-side)
5. [Generic vs Specific Records](#5-generic-vs-specific-records)
6. [Schema Evolution](#6-schema-evolution)
7. [Subject Naming Strategies](#7-subject-naming-strategies)
8. [Performance Considerations](#8-performance-considerations)

---

## 1. What is Schema Registry?

Schema Registry is a **centralized schema store** that sits alongside Kafka. It serves as the
single source of truth for what data looks like on each topic.

```
┌─────────────┐         ┌──────────────────┐         ┌─────────────┐
│  Producer   │ ──────► │  Schema Registry │ ◄────── │  Consumer   │
│             │         │                  │         │             │
│ "What's the │         │  Stores schemas  │         │ "Give me    │
│  ID for     │         │  by subject +    │         │  schema     │
│  this       │         │  version         │         │  for ID 3"  │
│  schema?"   │         │                  │         │             │
└─────────────┘         └──────────────────┘         └─────────────┘
        │                                                    │
        ▼                                                    ▼
┌──────────────────────────────────────────────────────────────────┐
│                         Kafka Broker                              │
│   Messages contain: [magic byte][schema ID][avro binary data]    │
└──────────────────────────────────────────────────────────────────┘
```

**Key points:**
- Kafka itself knows nothing about schemas — it stores raw bytes
- Schema Registry is a separate HTTP service (default port 8081)
- Schemas are stored under **subjects** (e.g., `orders-avro-value`)
- Each subject can have multiple **versions** (for schema evolution)
- Each unique schema gets a globally unique **integer ID**

---

## 2. How Serialization Works (Producer Side)

When `KafkaTemplate.send()` is called, the `KafkaAvroSerializer` executes this sequence:

### Step-by-step flow:

```
Your Code                    KafkaAvroSerializer              Schema Registry
─────────                    ───────────────────              ───────────────
                                     │
kafkaTemplate.send(          ┌───────┴────────┐
  topic, key, record)  ───►  │ 1. record      │
                             │    .getSchema() │
                             │                 │
                             │ 2. compute      │
                             │    subject name │──── "orders-avro-value"
                             │                 │
                             │ 3. register/    │         POST /subjects/{subject}/versions
                             │    lookup       │ ──────► { "schema": "..." }
                             │    schema       │ ◄────── { "id": 3 }
                             │                 │
                             │ 4. serialize    │
                             │    to bytes     │
                             └───────┬────────┘
                                     │
                                     ▼
                             [0x00][00 00 00 03][binary avro data]
                                     │
                                     ▼
                              Kafka Broker (topic partition)
```

### Detailed breakdown:

**Step 1 — Extract schema:**
```java
// The serializer calls this internally:
Schema writerSchema = ((GenericContainer) record).getSchema();

// For GenericRecord: returns the Schema object you passed to GenericData.Record(schema)
// For SpecificRecord (OrderAvro): returns OrderAvro.SCHEMA$ (compiled into the class)
```

**Step 2 — Compute subject name:**
```java
// Default: TopicNameStrategy
String subject = topic + "-value";  // "orders-avro" + "-value" = "orders-avro-value"
```

**Step 3 — Register or lookup:**
```java
// Pseudo-code inside the serializer:
int schemaId = schemaRegistry.register(subject, writerSchema);
// If auto.register.schemas=true → registers new schemas
// If auto.register.schemas=false → only looks up, fails if not found
// Result is cached: Map<Schema, Integer> → no network call on subsequent sends
```

**Step 4 — Binary serialization:**
```java
// Avro binary encoding: no field names, no type tags, just values in schema-defined order
DatumWriter<Object> writer = new GenericDatumWriter<>(writerSchema);
writer.write(record, encoder);
// Fields are written in the ORDER defined in the schema:
// eventId → eventType → timestamp → orderId → customerId → productId → quantity → amount → status
```

---

## 3. The Wire Format

Every message produced with `KafkaAvroSerializer` follows the **Confluent Wire Format**:

```
Byte 0         Bytes 1-4           Bytes 5+
┌──────┐   ┌──────────────┐   ┌─────────────────────────────┐
│ 0x00 │   │ Schema ID    │   │ Avro binary-encoded data    │
│      │   │ (Big Endian) │   │ (no field names, no types)  │
└──────┘   └──────────────┘   └─────────────────────────────┘
 Magic      4-byte integer       Variable length
 byte       (e.g., 3)
```

### Why is this format used?

- **Magic byte (0x00):** Identifies this as Confluent wire format (vs raw Avro or other formats)
- **Schema ID (4 bytes):** Tells the consumer exactly which schema was used to serialize this message.
  The consumer fetches this schema from the registry to decode the binary data.
- **Avro binary data:** Ultra-compact encoding. Example for a string field:
  - JSON: `{"orderId": "ORD-123"}` → 24 bytes
  - Avro binary: `[length varint][UTF-8 bytes]` → 8 bytes

### Why not embed the schema in every message?

The schema for `OrderAvro` is ~500 bytes of JSON. If you produce 1M messages/sec, embedding it
in each message would add ~500MB/sec of overhead. The 4-byte ID replaces that entirely.

### Concrete example:

For an OrderAvro with orderId="ORD-123", status=CREATED, quantity=3:

```
Raw bytes (hex):
00                          ← magic byte
00 00 00 03                 ← schema ID = 3
[varint-len]ORD-123         ← orderId (string: length-prefixed UTF-8)
...                         ← remaining fields in schema order
00                          ← status enum index 0 = CREATED
```

---

## 4. How Deserialization Works (Consumer Side)

The `KafkaAvroDeserializer` reverses the process:

```
Kafka Broker                 KafkaAvroDeserializer            Schema Registry
────────────                 ─────────────────────            ───────────────
                                      │
[0x00][00 00 00 03]     ┌─────────────┴──────────────┐
[avro binary]     ───►  │ 1. Read magic byte (0x00)  │
                        │                            │
                        │ 2. Read schema ID (3)      │
                        │                            │       GET /schemas/ids/3
                        │ 3. Fetch writer schema     │ ────► { "schema": "..." }
                        │    from registry           │ ◄──── Schema JSON
                        │    (cached after 1st call) │
                        │                            │
                        │ 4. Determine reader schema │
                        │    (local class or generic)│
                        │                            │
                        │ 5. Avro schema resolution  │
                        │    + binary decode         │
                        └─────────────┬──────────────┘
                                      │
                                      ▼
                              Java object (GenericRecord or OrderAvro)
```

### The two schemas at play:

| Schema | Source | Role |
|--------|--------|------|
| **Writer schema** | Fetched from registry by ID | How the data was encoded (field order, types) |
| **Reader schema** | Local to the consumer | How the consumer wants to see the data |

### What happens when writer ≠ reader schema?

Avro's **schema resolution** handles mismatches:

```
Writer schema (v1):                Reader schema (v2):
─────────────────                  ────────────────
fields: [                          fields: [
  orderId: string                    orderId: string
  quantity: int                      quantity: int
  status: enum                       status: enum
]                                    shippingAddress: string (default: "")
                                   ]
```

Result: `shippingAddress` field gets filled with the default value `""` — no error, no data loss.

### `specific.avro.reader` property:

```yaml
consumer:
  properties:
    specific.avro.reader: true   # use generated class (OrderAvro) as reader schema
    # specific.avro.reader: false  # (default) return GenericRecord
```

- `true` → Deserializer uses `OrderAvro.SCHEMA$` as reader schema, returns `OrderAvro` instances
- `false` → Deserializer uses writer schema as both reader and writer, returns `GenericRecord`

---

## 5. Generic vs Specific Records

### GenericRecord approach (this project's `/sample` endpoint):

```java
// Schema loaded at runtime from .avsc file
Schema schema = new Schema.Parser().parse(inputStream);
GenericRecord order = new GenericData.Record(schema);

order.put("orderId", orderId);          // field name is a raw string
order.put("quantity", 3);               // type is Object — no compile check
order.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "CREATED"));
```

### SpecificRecord approach (this project's `/sample-specific` endpoint):

```java
// No schema loading — it's embedded in the generated class
OrderAvro order = OrderAvro.newBuilder()
    .setOrderId(orderId)                // typed setter — String required
    .setQuantity(3)                     // typed setter — int required
    .setStatus(OrderStatusAvro.CREATED) // enum type — compile-time checked
    .build();
```

### Comparison table:

| Aspect | GenericRecord | SpecificRecord (Generated Class) |
|--------|-------------|----------------------------------|
| **Schema source** | Loaded at runtime (file, registry, etc.) | Compiled into the class at build time |
| **Type safety** | None — all values are `Object` | Full — getters/setters are typed |
| **Field access** | `record.get("orderId")` returns `Object` | `order.getOrderId()` returns `String` |
| **Typo detection** | Runtime failure (or silent null) | Compile error |
| **IDE support** | No autocomplete for fields | Full autocomplete + refactoring |
| **Enum handling** | `new GenericData.EnumSymbol(schema, "CREATED")` | `OrderStatusAvro.CREATED` |
| **Code generation needed** | No | Yes (Gradle Avro plugin) |
| **Schema flexibility** | Can handle any schema at runtime | Tied to the schema at compile time |
| **Use cases** | Schema routing, dynamic topics, tooling | Business logic, domain services |

### Impact on serialization/deserialization:

**Serialization (producer):**
Both approaches produce **identical bytes on the wire**. The serializer calls `record.getSchema()`
regardless — GenericRecord returns the schema you passed in, SpecificRecord returns the compiled
`SCHEMA$` field. The Avro binary encoding is the same either way.

```
GenericRecord  ──► KafkaAvroSerializer ──► [0x00][schema ID][binary] ──► Kafka
SpecificRecord ──► KafkaAvroSerializer ──► [0x00][schema ID][binary] ──► Kafka
                     (identical output)
```

**Deserialization (consumer):**
This is where the difference matters most:

```java
// specific.avro.reader=false (GenericRecord):
GenericRecord record = (GenericRecord) message.value();
String orderId = record.get("orderId").toString();  // returns Object, must cast
int quantity = (int) record.get("quantity");         // risky cast

// specific.avro.reader=true (SpecificRecord):
OrderAvro order = (OrderAvro) message.value();
String orderId = order.getOrderId();                // returns CharSequence
int quantity = order.getQuantity();                  // returns int directly
OrderStatusAvro status = order.getStatus();         // returns typed enum
```

### When to use which:

**Use GenericRecord when:**
- Building generic Kafka tooling (schema-agnostic connectors, audit logs)
- Schema is not known at compile time (dynamic routing)
- You want to avoid code generation in your build pipeline
- You're reading from topics where you don't control the schema

**Use SpecificRecord when:**
- Building business logic that operates on known data structures
- You want IDE support, refactoring safety, and compile-time checks
- You're in a domain service that owns the schema
- Team prefers strong typing and wants bugs caught early

---

## 6. Schema Evolution

Schema Registry enforces **compatibility rules** that control how schemas can change over time.

### Compatibility modes:

| Mode | Rule | Example allowed change |
|------|------|----------------------|
| **BACKWARD** (default) | New schema can read old data | Add a field with a default value |
| **FORWARD** | Old schema can read new data | Remove a field that has a default |
| **FULL** | Both directions work | Add/remove fields with defaults |
| **NONE** | No checks | Any change (dangerous) |

### How it works with Schema Registry:

```
Version 1 (registered):           Version 2 (attempting to register):
─────────────────────              ──────────────────────────────────
{                                  {
  "fields": [                        "fields": [
    {"name": "orderId", ...},          {"name": "orderId", ...},
    {"name": "quantity", ...}          {"name": "quantity", ...},
  ]                                    {"name": "priority",        ← NEW FIELD
}                                         "type": "string",
                                          "default": "NORMAL"}     ← HAS DEFAULT
                                     ]
                                   }
```

When the producer tries to register v2:
1. Registry fetches the latest version (v1) for the subject
2. Checks if v2 is BACKWARD compatible with v1
3. New field has a default → old consumers can still read new data (default fills in)
4. Registration succeeds → v2 gets a new schema ID

**Without a default value, BACKWARD compatibility check fails and registration is rejected.**

### Evolution in practice (this project):

If you add a field to `order.avsc`:
```json
{"name": "priority", "type": "string", "default": "NORMAL"}
```

Then:
- Rebuild generates a new `OrderAvro` class with `getPriority()` / `setPriority()`
- Producer serializes with the new schema → gets a new schema ID
- Old consumers (without the field) still work — Avro resolution ignores unknown fields
- New consumers get the field populated from the message
- Messages written with the old schema → new consumers see `"NORMAL"` (the default)

---

## 7. Subject Naming Strategies

The **subject** determines how schemas are grouped and versioned in the registry.

### Available strategies:

| Strategy | Subject Name | Schema Scope |
|----------|-------------|--------------|
| **TopicNameStrategy** (default) | `<topic>-value` | One schema per topic |
| **RecordNameStrategy** | `<namespace>.<record>` | Schema shared across topics |
| **TopicRecordNameStrategy** | `<topic>-<namespace>.<record>` | Per-topic, per-record |

### TopicNameStrategy (this project):

```yaml
# Implicit default — no configuration needed
# Subject = "orders-avro-value"
```

All messages on `orders-avro` topic must conform to the same schema lineage.

### RecordNameStrategy:

```yaml
producer:
  properties:
    value.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
# Subject = "com.scaleatdesign.kafka.avro.OrderAvro"
```

Useful when multiple event types share a topic (e.g., an event bus pattern).

---

## 8. Performance Considerations

### Caching behavior:

```
First message:
  Producer → Schema Registry (HTTP POST) → gets schema ID → caches it
  ~5-20ms network overhead

Subsequent messages (same schema):
  Producer → local cache hit → schema ID already known
  ~0ms overhead (just memory lookup)
```

The serializer maintains:
- `Map<Schema, Integer>` — schema → ID cache (producer side)
- `Map<Integer, Schema>` — ID → schema cache (consumer side)

### Binary encoding efficiency:

Avro binary is significantly more compact than JSON:

```
JSON message:  {"eventId":"abc","eventType":"ORDER_CREATED","timestamp":1717849200000,
                "orderId":"ORD-123","customerId":"CUST-456","productId":"PROD-789",
                "quantity":3,"amount":14999,"status":"CREATED"}
≈ 230 bytes

Avro binary:   [varint+UTF8][varint+UTF8][varint][varint+UTF8]...
≈ 75 bytes (67% smaller)
```

### Generic vs Specific performance:

| Operation | GenericRecord | SpecificRecord |
|-----------|--------------|----------------|
| Serialization speed | ~same | ~same |
| Deserialization speed | Slightly slower (reflection-based) | Slightly faster (direct field access) |
| Memory allocation | More objects (boxed primitives) | Fewer objects (primitive fields) |
| GC pressure | Higher | Lower |

The difference is marginal (<5%) for most workloads. Choose based on **developer ergonomics**,
not performance.

---

## Summary: The Complete Flow

```
                          BUILD TIME
                          ══════════
order.avsc ──(Gradle Avro Plugin)──► OrderAvro.java (generated)
                                      │
                                      │ contains SCHEMA$ field
                                      ▼
                          RUNTIME (Producer)
                          ═════════════════
OrderAvro instance ──► KafkaAvroSerializer
                        │
                        ├─ 1. getSchema() → extracts SCHEMA$
                        ├─ 2. subject = "orders-avro-value"
                        ├─ 3. POST schema to registry → gets ID (cached)
                        ├─ 4. write [0x00][ID][binary] to Kafka
                        │
                        ▼
                   Kafka Topic (raw bytes, schema-ignorant)
                        │
                        ▼
                          RUNTIME (Consumer)
                          ═════════════════
                   KafkaAvroDeserializer
                        │
                        ├─ 1. read magic byte + schema ID
                        ├─ 2. GET schema from registry by ID (cached)
                        ├─ 3. writer schema = from registry
                        ├─ 4. reader schema = OrderAvro.SCHEMA$ (if specific.avro.reader=true)
                        ├─ 5. Avro resolution: match writer→reader fields
                        └─ 6. return OrderAvro instance (or GenericRecord)
```

---

## 9. How getSchema() Works — Where the Schema Lives Inside the Record

A common question: "When `KafkaAvroSerializer` calls `record.getSchema()`, where does the schema
come from? We never explicitly passed it to the serializer."

The answer: **the schema is embedded inside the record object itself**, not passed to the serializer.

### The interface contract:

Both GenericRecord and SpecificRecord implement `org.apache.avro.generic.GenericContainer`:

```java
public interface GenericContainer {
    Schema getSchema();  // ← this is what the serializer calls
}
```

The serializer simply calls this method on whatever object you hand it:

```java
// Inside KafkaAvroSerializer.serialize() (simplified):
public byte[] serialize(String topic, Object record) {
    Schema schema = ((GenericContainer) record).getSchema();  // polymorphic call
    // ... rest of serialization
}
```

### GenericRecord — schema passed at construction time:

```java
// YOUR CODE:
Schema schema = new Schema.Parser().parse(inputStream);   // you load the schema
GenericRecord order = new GenericData.Record(schema);      // you pass it HERE
```

Internally, `GenericData.Record` stores it as a field:

```java
// INSIDE AVRO'S SOURCE CODE (GenericData.Record):
public class Record implements GenericRecord {
    private final Schema schema;          // ← stored as instance field

    public Record(Schema schema) {
        this.schema = schema;             // ← saved at construction
        // also allocates Object[] for field values based on schema.getFields().size()
    }

    @Override
    public Schema getSchema() {
        return this.schema;               // ← serializer reads it here
    }
}
```

The schema is stored **per-instance**. Every `GenericData.Record` carries a reference to its schema.

### SpecificRecord (OrderAvro) — schema baked in at build time:

The Gradle Avro plugin generates a class from `order.avsc` that looks like this:

```java
// AUTO-GENERATED FILE: build/generated-main-avro-java/com/scaleatdesign/kafka/avro/OrderAvro.java
public class OrderAvro extends SpecificRecordBase implements SpecificRecord {

    // Schema parsed once, stored as a static constant
    public static final Schema SCHEMA$ = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"OrderAvro\",\"namespace\":\"com.scaleatdesign.kafka.avro\","
        + "\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventType\","
        + "\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\","
        + "\"logicalType\":\"timestamp-millis\"}},...]}"
    );

    @Override
    public Schema getSchema() {
        return SCHEMA$;                   // ← serializer reads it here
    }

    // typed getters/setters for each field...
    public String getOrderId() { return orderId; }
    public void setOrderId(String value) { this.orderId = value; }
    // ...
}
```

The schema is stored **per-class** (static). All instances share the same `SCHEMA$` reference.
You never load or pass it — the code generator did it for you.

### Visual comparison:

```
GenericRecord path:
─────────────────
order.avsc ──(you load at runtime)──► Schema object
                                          │
                                          ▼
                              new GenericData.Record(schema)
                                          │
                                          │  schema stored as instance field
                                          ▼
                              record.getSchema() → returns that instance field


SpecificRecord path:
────────────────────
order.avsc ──(Gradle plugin at build time)──► OrderAvro.java source code
                                                  │
                                                  │  schema JSON string literal
                                                  │  embedded in SCHEMA$ static field
                                                  ▼
                                          new OrderAvro()
                                                  │
                                                  │  getSchema() returns SCHEMA$
                                                  ▼
                                          record.getSchema() → returns class-level static
```

### Key insight:

The serializer is **schema-agnostic** — it doesn't know or care where the schema originated.
It relies entirely on the `GenericContainer.getSchema()` contract. This is why both Generic
and Specific records work interchangeably with `KafkaTemplate<String, GenericRecord>`:

```java
private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

// Both work because both implement GenericContainer:
kafkaTemplate.send(topic, key, genericRecord);   // getSchema() → instance field
kafkaTemplate.send(topic, key, specificRecord);  // getSchema() → static SCHEMA$
```

The serializer treats them identically from that point forward.

---

## 10. TopicNameStrategy — Where It's Defined in Our Code

### Short answer: It's NOT explicitly defined — it's the default.

In this project's `application.yml`, there is **no** `value.subject.name.strategy` property:

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://localhost:8081
        auto.register.schemas: true
        # ← No value.subject.name.strategy here!
```

When this property is absent, `KafkaAvroSerializer` falls back to its built-in default:

```java
// Inside Confluent's AbstractKafkaSchemaSerDe (parent class of KafkaAvroSerializer):
public static final String SUBJECT_NAME_STRATEGY = "value.subject.name.strategy";
public static final String SUBJECT_NAME_STRATEGY_DEFAULT =
    "io.confluent.kafka.serializers.subject.TopicNameStrategy";
```

### How TopicNameStrategy computes the subject:

```java
// Simplified implementation of TopicNameStrategy:
public class TopicNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        if (isKey) {
            return topic + "-key";      // e.g., "orders-avro-key"
        } else {
            return topic + "-value";    // e.g., "orders-avro-value"
        }
    }
}
```

Note: it **ignores the schema entirely** — only the topic name determines the subject.

### Tracing it through our code:

```
AvroOrderProducer.java:
────────────────────────
kafkaTemplate.send(KafkaTopics.ORDERS_AVRO, orderId, orderAvro)
                        │
                        │  topic = "orders-avro" (from KafkaTopics constant)
                        ▼
KafkaAvroSerializer.serialize("orders-avro", ..., record):
                        │
                        │  strategy = TopicNameStrategy (default, not configured)
                        │  subject  = strategy.subjectName("orders-avro", isKey=false, schema)
                        │           = "orders-avro" + "-value"
                        │           = "orders-avro-value"
                        ▼
POST http://localhost:8081/subjects/orders-avro-value/versions
     Body: { "schema": "<OrderAvro schema JSON>" }
```

### If you wanted to override it:

```yaml
# application.yml — explicitly set (same as default):
spring:
  kafka:
    producer:
      properties:
        schema.registry.url: http://localhost:8081
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicNameStrategy

# Or switch to RecordNameStrategy (for multi-schema topics):
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
        # → subject = "com.scaleatdesign.kafka.avro.OrderAvro" (uses schema's fullName)

# Or TopicRecordNameStrategy:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
        # → subject = "orders-avro-com.scaleatdesign.kafka.avro.OrderAvro"
```

### When would you change from the default?

| Scenario | Strategy to use | Why |
|----------|----------------|-----|
| One schema per topic (our case) | TopicNameStrategy (default) | Simple, one compatibility lineage per topic |
| Multiple event types on one topic | RecordNameStrategy | Each record type gets its own subject/compatibility |
| Multiple event types, want per-topic isolation | TopicRecordNameStrategy | Combines both — per-topic AND per-record |

### Consumer side also has a strategy:

```yaml
spring:
  kafka:
    consumer:
      properties:
        # Also defaults to TopicNameStrategy if not set
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicNameStrategy
```

The consumer's strategy must match the producer's — otherwise the deserializer would look up
the schema under the wrong subject name and fail.

### Summary for this project:

- **Where is it defined?** Nowhere explicitly — it relies on the Confluent library's default.
- **What is the default?** `TopicNameStrategy` → subject = `<topic>-value`
- **What subject does our producer register under?** `orders-avro-value`
- **Would you need to change it?** Only if you put multiple event types on the same topic.

---

## 11. RecordNameStrategy & TopicRecordNameStrategy — Deep Dive

### RecordNameStrategy

Uses the **schema's full name** (namespace + record name) as the subject. The topic name is
completely ignored.

```java
// Simplified implementation:
public class RecordNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        // Ignores topic entirely — uses schema's qualified name
        return schema.name();  // e.g., "com.scaleatdesign.kafka.avro.OrderAvro"
    }
}
```

**How it works in practice:**

Imagine an event bus topic that carries multiple event types:

```
Topic: "events" (single topic, multiple schemas)
  │
  ├── Producer sends OrderAvro
  │     → subject: "com.scaleatdesign.kafka.avro.OrderAvro"
  │
  ├── Producer sends PaymentAvro
  │     → subject: "com.scaleatdesign.kafka.avro.PaymentAvro"
  │
  └── Producer sends ShipmentAvro
        → subject: "com.scaleatdesign.kafka.avro.ShipmentAvro"
```

Each record type gets **its own subject** in the registry. This means:
- `OrderAvro` can evolve independently of `PaymentAvro`
- Compatibility checks are per-record-type, not per-topic
- Adding a new event type doesn't affect existing ones

**Configuration:**

```yaml
spring:
  kafka:
    producer:
      properties:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
    consumer:
      properties:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
```

**Key behavior — schema is shared across topics:**

If `OrderAvro` is published to multiple topics, they all share **one subject**:

```
Topic: "orders"       ──► subject: "com.scaleatdesign.kafka.avro.OrderAvro"
Topic: "orders-retry" ──► subject: "com.scaleatdesign.kafka.avro.OrderAvro"  (SAME!)
Topic: "orders-dlq"   ──► subject: "com.scaleatdesign.kafka.avro.OrderAvro"  (SAME!)
```

This means a schema change registered from any of these topics affects ALL of them.
That's the tradeoff — global schema sharing.

**When to use:**
- Event bus / event sourcing with one topic carrying multiple event types
- You want each event type to evolve independently
- You're okay with schema being globally shared regardless of which topic it's on

---

### TopicRecordNameStrategy

Combines **both** the topic name and the schema's full name into the subject.
Maximum isolation — per-record AND per-topic.

```java
// Simplified implementation:
public class TopicRecordNameStrategy implements SubjectNameStrategy {

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        return topic + "-" + schema.name();
        // e.g., "events-com.scaleatdesign.kafka.avro.OrderAvro"
    }
}
```

**How it works in practice:**

```
Topic: "events" (multi-schema topic)
  │
  ├── OrderAvro   → subject: "events-com.scaleatdesign.kafka.avro.OrderAvro"
  ├── PaymentAvro → subject: "events-com.scaleatdesign.kafka.avro.PaymentAvro"
  └── ShipmentAvro→ subject: "events-com.scaleatdesign.kafka.avro.ShipmentAvro"

Topic: "orders-dlq" (same record type, different topic)
  │
  └── OrderAvro   → subject: "orders-dlq-com.scaleatdesign.kafka.avro.OrderAvro"
                              ^^^^^^^^^^^ different subject!
```

**Key difference from RecordNameStrategy:**

The same record type on different topics gets **different subjects**:

```
RecordNameStrategy:
  "orders" + OrderAvro    → "com.scaleatdesign.kafka.avro.OrderAvro"
  "orders-dlq" + OrderAvro → "com.scaleatdesign.kafka.avro.OrderAvro"  ← SAME subject
  (schema change on one affects the other)

TopicRecordNameStrategy:
  "orders" + OrderAvro    → "orders-com.scaleatdesign.kafka.avro.OrderAvro"
  "orders-dlq" + OrderAvro → "orders-dlq-com.scaleatdesign.kafka.avro.OrderAvro"  ← DIFFERENT
  (schemas evolve independently per topic)
```

**Configuration:**

```yaml
spring:
  kafka:
    producer:
      properties:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
    consumer:
      properties:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
```

**When to use:**
- Multiple event types per topic (like RecordNameStrategy)
- AND you want the same record type to evolve independently on different topics
- Maximum isolation — nothing is shared
- Common in large organizations where different teams own different topics

---

### Side-by-side comparison with a concrete scenario:

Given: `OrderAvro` (namespace `com.scaleatdesign.kafka.avro`) sent to topic `orders-avro`

| Strategy | Subject Name | What shares this subject? |
|----------|-------------|--------------------------|
| TopicNameStrategy | `orders-avro-value` | ALL records on `orders-avro` topic |
| RecordNameStrategy | `com.scaleatdesign.kafka.avro.OrderAvro` | ALL `OrderAvro` messages on ANY topic |
| TopicRecordNameStrategy | `orders-avro-com.scaleatdesign.kafka.avro.OrderAvro` | Only `OrderAvro` on `orders-avro` topic |

### Decision tree:

```
How many schema types per topic?
│
├── ONE schema per topic (most common)
│     └── TopicNameStrategy ✓ (default, simplest)
│
└── MULTIPLE schemas per topic (event bus)
      │
      ├── Same record type on multiple topics should share evolution?
      │     └── YES → RecordNameStrategy ✓
      │           (e.g., OrderAvro evolves globally regardless of topic)
      │
      └── Same record type on different topics should evolve independently?
            └── YES → TopicRecordNameStrategy ✓
                  (e.g., OrderAvro on "orders" can be v3 while on "orders-dlq" stays v1)
```

### Important: Producer and Consumer MUST match

Whatever strategy the producer uses, the consumer MUST use the same one. Otherwise the
consumer's deserializer will compute a different subject name and fail to find the schema:

```
Producer uses RecordNameStrategy:
  → registers under "com.scaleatdesign.kafka.avro.OrderAvro"

Consumer uses TopicNameStrategy (mismatch!):
  → looks up under "orders-avro-value"
  → FAILS: schema not found under that subject
```

Always configure both sides consistently:

```yaml
spring:
  kafka:
    producer:
      properties:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
    consumer:
      properties:
        value.subject.name.strategy: io.confluent.kafka.serializers.subject.RecordNameStrategy
```

---

## 12. auto.register.schemas=false — Who Registers the Schema First Time?

With `auto.register.schemas=false`, the serializer **only looks up** schemas — it never registers.
If the schema isn't already in the registry, the producer throws:

```
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException:
  Schema not found; error code: 40403
```

So the schema must be registered **before** the application starts producing.

### Who registers it? You do — outside the application.

---

### Approach 1: CI/CD Pipeline (most common in production)

Register schemas as a deployment step, after build but before deploy:

```bash
# In CI/CD pipeline (e.g., GitHub Actions, Jenkins, GitLab CI):
curl -X POST http://schema-registry:8081/subjects/orders-avro-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"OrderAvro\",\"namespace\":\"com.scaleatdesign.kafka.avro\",\"fields\":[...]}"}'
```

Or using a **Gradle plugin** (more maintainable):

```groovy
// build.gradle
plugins {
    id 'com.github.imflog.kafka-schema-registry-gradle-plugin' version '1.12.0'
}

schemaRegistry {
    url = 'http://schema-registry:8081'
    register {
        subject('orders-avro-value', 'src/main/avro/order.avsc', 'AVRO')
    }
}
```

Then in CI:
```bash
./gradlew registerSchemasTask   # registers schema before deploying the app
```

**Typical CI/CD flow:**

```
┌─────────┐    ┌───────────────┐    ┌────────────────────┐    ┌──────────────┐
│  Build  │───►│ Run Tests     │───►│ Register Schema    │───►│ Deploy App   │
│         │    │               │    │ (to registry)      │    │              │
└─────────┘    └───────────────┘    └────────────────────┘    └──────────────┘
                                           │
                                           ▼
                                    Schema Registry now has
                                    the schema → app can
                                    produce safely
```

---

### Approach 2: Schema Registry REST API (manual / one-off)

For initial setup or testing:

```bash
# Register directly from the .avsc file:
cat src/main/avro/order.avsc | jq -c '{schema: (. | tostring)}' | \
  curl -X POST http://localhost:8081/subjects/orders-avro-value/versions \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d @-

# Response: {"id": 1}  ← globally unique schema ID
```

Or register with explicit compatibility check:

```bash
# First, test compatibility before registering:
curl -X POST http://localhost:8081/compatibility/subjects/orders-avro-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'

# Response: {"is_compatible": true}

# Then register:
curl -X POST http://localhost:8081/subjects/orders-avro-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'
```

---

### Approach 3: Dedicated Schema Management Service

Some organizations have a separate service or admin tool that:
- Reads `.avsc` files from a schema repository (Git)
- Validates compatibility
- Registers schemas on promotion (dev → staging → prod)
- Provides an approval workflow before schema changes go live

---

### Why disable auto-registration in production?

```
auto.register.schemas=true (dev/test):
  ✓ Convenient — producer auto-registers on first send
  ✗ Risky — any producer instance can register any schema
  ✗ No review process — a buggy deploy could register an incompatible schema

auto.register.schemas=false (production):
  ✓ Controlled — only CI/CD can register schemas
  ✓ Predictable — schema changes go through code review
  ✓ Safe — producers can only use pre-approved schemas
  ✗ Requires explicit registration step in deployment
```

**The risk with auto-registration:**

```
Developer pushes broken schema change → deploys to production
  → producer auto-registers incompatible schema v2
  → existing consumers CRASH trying to read new messages
  → rollback needed, but schema v2 is already registered
  → manual cleanup in Schema Registry required
```

With `auto.register.schemas=false`, this scenario is impossible — the producer would fail
to start (can't find schema) rather than corrupting the registry.

---

### Typical environment strategy:

| Environment | auto.register.schemas | Who registers? | Why |
|-------------|----------------------|----------------|-----|
| Local dev | `true` | Producer auto-registers | Fast iteration, no friction |
| CI tests | `true` | Tests auto-register | Isolated, disposable registry |
| Staging | `false` | CI/CD pipeline | Catch issues before prod |
| Production | `false` | CI/CD pipeline | Controlled, auditable changes |

### Configuration per environment (Spring profiles):

```yaml
# application.yml (default / dev):
spring:
  kafka:
    producer:
      properties:
        auto.register.schemas: true

---
# application-staging.yml:
spring:
  kafka:
    producer:
      properties:
        auto.register.schemas: false

---
# application-prod.yml:
spring:
  kafka:
    producer:
      properties:
        auto.register.schemas: false
```

### What happens at runtime with auto.register.schemas=false:

```
Producer starts → KafkaAvroSerializer initialized
                        │
First message sent      │
                        ▼
        serializer.serialize(topic, record):
            schema = record.getSchema()
            subject = "orders-avro-value"
            │
            ├── auto.register.schemas=false
            │   → calls: GET /subjects/orders-avro-value/versions
            │   → looks for this exact schema in the subject
            │
            ├── Schema FOUND → returns schema ID → serialization proceeds ✓
            │
            └── Schema NOT FOUND → throws RestClientException (40403) ✗
                → message send fails
                → producer cannot produce until schema is registered externally
```
