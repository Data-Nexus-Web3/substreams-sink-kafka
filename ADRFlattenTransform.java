// Custom Kafka Connect Transform for ADR flattening
public class ADRFlattenTransform implements Transformation<SinkRecord> {
    
    @Override
    public SinkRecord apply(SinkRecord record) {
        // Extract ADR message
        Struct adrMessage = (Struct) record.value();
        
        // Create flattened structure
        Map<String, Object> flattened = new HashMap<>();
        
        // Top-level fields
        flattened.put("network", adrMessage.getString("network"));
        flattened.put("version", adrMessage.getString("version"));
        flattened.put("prev_cursor", adrMessage.getString("prev_cursor"));
        
        // Flatten metadata
        Struct metadata = adrMessage.getStruct("metadata");
        flattened.put("block_number", metadata.getInt64("block_number"));
        flattened.put("block_hash", metadata.getString("block_hash"));
        flattened.put("block_timestamp", metadata.getInt64("block_timestamp"));
        
        // Flatten each transaction into separate records
        List<Struct> transactions = adrMessage.getArray("transactions");
        List<SinkRecord> records = new ArrayList<>();
        
        for (int i = 0; i < transactions.size(); i++) {
            Map<String, Object> txFlattened = new HashMap<>(flattened);
            Struct tx = transactions.get(i);
            
            txFlattened.put("tx_index", i);
            txFlattened.put("tx_hash", tx.getString("hash"));
            txFlattened.put("tx_from", tx.getString("from"));
            txFlattened.put("tx_to", tx.getString("to"));
            txFlattened.put("tx_value", tx.getString("value"));
            // ... more transaction fields
            
            records.add(record.newRecord(
                record.topic() + "-transactions",
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null, // New flattened schema
                txFlattened,
                record.timestamp()
            ));
        }
        
        return records; // Return multiple records per ADR message
    }
}