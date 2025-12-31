CREATE OR REPLACE PROCEDURE update_datasource_events_processing_status(
  p_job_id UUID
)
LANGUAGE plpgsql
AS '
DECLARE
  index_exists BOOLEAN;
BEGIN
  -- Check if an index exists on the job_id column
  SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE 
      tablename = ''datasource_event'' AND
      indexname = ''idx_datasource_event_job_id''
  ) INTO index_exists;
  
  -- If no index exists on job_id, create one
  IF NOT index_exists THEN
    CREATE INDEX idx_datasource_event_job_id ON datasource_event(job_id);
  END IF;

  -- Check if an index exists on the job_id column
  SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE 
      tablename = ''datasource_event'' AND
      indexname = ''idx_datasource_event_job_status''
  ) INTO index_exists;
  
  -- If no index exists on job_id, create one
  IF NOT index_exists THEN
    CREATE INDEX idx_datasource_event_job_status ON datasource_event(job_id, status);
  END IF;

  UPDATE datasource_event
  SET 
    forecasted = true,
    status = ''PROCESSING''
  WHERE 
    job_id = p_job_id
    AND status != ''PROCESSING_ERROR'';
END;
';


CREATE OR REPLACE PROCEDURE update_datasource_events_processing_completed_status(
  p_job_id UUID
)
LANGUAGE plpgsql
AS '
DECLARE
  index_exists BOOLEAN;
BEGIN
  -- Check if an index exists on the job_id column
  SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE 
      tablename = ''datasource_event'' AND
      indexname = ''idx_datasource_event_job_id''
  ) INTO index_exists;
  
  -- If no index exists on job_id, create one
  IF NOT index_exists THEN
    CREATE INDEX idx_datasource_event_job_id ON datasource_event(job_id);
  END IF;

  -- Check if an index exists on the job_id column
  SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE 
      tablename = ''datasource_event'' AND
      indexname = ''idx_datasource_event_job_status''
  ) INTO index_exists;
  
  -- If no index exists on job_id, create one
  IF NOT index_exists THEN
    CREATE INDEX idx_datasource_event_job_status ON datasource_event(job_id, status);
  END IF;

  UPDATE datasource_event
  SET 
    recommended = true,
    status = ''PROCESSED''
  WHERE 
    job_id = p_job_id
    AND status != ''PROCESSING_ERROR'';
END;
';


CREATE OR REPLACE PROCEDURE create_datasource_event_commit_datetime_index()
LANGUAGE plpgsql
AS '
BEGIN
  CREATE INDEX IF NOT EXISTS idx_datasource_event_commit_datetime ON datasource_event(commit_date_time);
  RAISE NOTICE ''Index on datasource_event.commit_date_time created or already exists'';
END;
';

CREATE OR REPLACE PROCEDURE deduplicate_commit_datetimes(
    event_ids_string VARCHAR
)
LANGUAGE plpgsql
AS '
DECLARE
    duplicate_group RECORD;
    event_record RECORD;
    offset_ms INTEGER;
    event_ids_array BIGINT[];
BEGIN
    -- Ensure index exists for performance
    CALL create_datasource_event_commit_datetime_index();
    
    -- Convert comma-delimited string to array
    event_ids_array := string_to_array(event_ids_string, '','')::BIGINT[];
    
    -- Process each group of records that share the same commit_date_time
    FOR duplicate_group IN 
        SELECT commit_date_time, array_agg(id ORDER BY id) as ids
        FROM datasource_event 
        WHERE id = ANY(event_ids_array)
        GROUP BY commit_date_time
        HAVING COUNT(*) > 1
    LOOP
        -- For each duplicate group, update all but the first record
        offset_ms := 1;
        
        FOR event_record IN 
            SELECT unnest(duplicate_group.ids[2:]) as event_id
        LOOP
            UPDATE datasource_event 
            SET commit_date_time = duplicate_group.commit_date_time + INTERVAL ''1 millisecond'' * offset_ms
            WHERE id = event_record.event_id;
            
            offset_ms := offset_ms + 1;
        END LOOP;
        
        RAISE NOTICE ''Updated % records with commit_date_time %'', 
                     array_length(duplicate_group.ids, 1) - 1, 
                     duplicate_group.commit_date_time;
    END LOOP;
END;
';


