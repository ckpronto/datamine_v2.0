-- PostgreSQL Optimization for 100-CPU VM with 288GB RAM
-- Apply these settings for optimal performance with massive datasets

-- ============================================================================
-- PARALLEL PROCESSING SETTINGS (100 CPU Optimization)
-- ============================================================================

-- Set maximum worker processes to utilize all CPUs
-- Requires PostgreSQL restart
-- ALTER SYSTEM SET max_worker_processes = 100;

-- Set parallel workers for queries (80% of CPUs for queries, 20% for maintenance)
ALTER SYSTEM SET max_parallel_workers = 80;

-- Set parallel workers per gather node (optimal for large bulk operations)
ALTER SYSTEM SET max_parallel_workers_per_gather = 16;

-- Set parallel workers for maintenance operations (ANALYZE, CREATE INDEX, etc.)
ALTER SYSTEM SET max_parallel_maintenance_workers = 16;

-- ============================================================================
-- MEMORY SETTINGS (288GB RAM Optimization)
-- ============================================================================

-- Set shared buffers to 25% of RAM (72GB) - requires restart
-- ALTER SYSTEM SET shared_buffers = '72GB';

-- Set effective cache size to 75% of RAM (216GB) - helps query planner
ALTER SYSTEM SET effective_cache_size = '216GB';

-- Set work memory for each operation (2GB per worker for complex operations)
ALTER SYSTEM SET work_mem = '2GB';

-- Set maintenance work memory (16GB for bulk operations like CREATE INDEX)
ALTER SYSTEM SET maintenance_work_mem = '16GB';

-- ============================================================================
-- BULK OPERATION OPTIMIZATIONS
-- ============================================================================

-- Optimize checkpoint behavior for bulk loads
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET min_wal_size = '2GB';
ALTER SYSTEM SET max_wal_size = '16GB';

-- Optimize for bulk operations
ALTER SYSTEM SET synchronous_commit = off;
ALTER SYSTEM SET commit_delay = 10000;
ALTER SYSTEM SET commit_siblings = 10;

-- ============================================================================
-- I/O AND STORAGE OPTIMIZATIONS
-- ============================================================================

-- Optimize for SSD storage
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET seq_page_cost = 1.0;

-- Optimize I/O operations
ALTER SYSTEM SET effective_io_concurrency = 100;
ALTER SYSTEM SET maintenance_io_concurrency = 50;

-- ============================================================================
-- APPLY CONFIGURATION AND RESTART INFO
-- ============================================================================

-- Reload configuration for settings that don't require restart
SELECT pg_reload_conf();

-- Display current settings after reload
SELECT name, setting, unit, context, pending_restart FROM pg_settings 
WHERE name IN (
    'max_worker_processes', 
    'max_parallel_workers', 
    'max_parallel_workers_per_gather', 
    'max_parallel_maintenance_workers',
    'shared_buffers',
    'effective_cache_size',
    'work_mem',
    'maintenance_work_mem'
) ORDER BY name;