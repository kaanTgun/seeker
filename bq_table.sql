-- This schema is based on the provided PostgreSQL schema, adapted for Google BigQuery.
-- BigQuery does not enforce Primary Key or Foreign Key constraints at the DML level,
-- but they can be defined for metadata purposes and potential query optimization.
-- UUID fields are mapped to BYTES or STRING in BigQuery. STRING is generally recommended
-- for ease of use and compatibility with BigQuery's GENERATE_UUID() function,
-- although BYTES is more space-efficient. We will use STRING here for readability
-- and common practice with UUIDs in BigQuery.
-- TIMESTAMP WITH TIME ZONE is mapped to TIMESTAMP in BigQuery.

-- Table for Audio
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.AUDIO` (
    id STRING NOT NULL, -- Mapped from UUID, using STRING for BigQuery compatibility
    gcsBucket STRING NOT NULL,
    gcsObjectPath STRING NOT NULL,
    fileSize INT64 -- Mapped from INT
);

-- Table for Shows
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.SHOWS` (
    id STRING NOT NULL, -- Mapped from UUID
    title STRING NOT NULL,
    sanitizedTitle STRING,
    description STRING, -- Mapped from TEXT
    imageUrl STRING,    -- Mapped from TEXT
    rssUrl STRING,      -- Mapped from TEXT
    websiteUrl STRING,  -- Mapped from TEXT
    language STRING,    -- Mapped from VARCHAR(10)
    tags ARRAY<STRING>,  --  ARRAY<STRING> for a list of tags
    lastUpdated TIMESTAMP NOT NULL -- Mapped from TIMESTAMP WITH TIME ZONE
);

-- Table for Episodes
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.EPISODES` (
    id STRING NOT NULL, -- Mapped from UUID
    showId STRING NOT NULL, -- Mapped from UUID
    title STRING NOT NULL,
    sanitizedTitle STRING,
    description STRING, -- Mapped from TEXT
    publishedDate TIMESTAMP, -- Mapped from TIMESTAMP WITH TIME ZONE
    durationSeconds INT64, -- Mapped from INT
    originalAudioUrl STRING, -- Mapped from TEXT
    audioId STRING -- Mapped from UUID, allows NULL as per original schema
);

-- Table for People
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.PEOPLE` (
    id STRING NOT NULL, -- Mapped from UUID
    full_name STRING NOT NULL,
    aliases STRING, -- Mapped from TEXT
    audioId STRING -- Mapped from UUID, allows NULL as per original schema
);

-- Table for Topics
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.TOPICS` (
    id STRING NOT NULL, -- Mapped from UUID
    episodeId STRING NOT NULL, -- Mapped from UUID
    title STRING NOT NULL,
    start_ms INT64 NOT NULL, -- Mapped from INT
    end_ms INT64 NOT NULL -- Mapped from INT
);

-- Table for Synthetic Topics
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.SYNTHETIC_TOPICS` (
    id STRING NOT NULL, -- Mapped from UUID
    episodeId STRING NOT NULL, -- Mapped from UUID
    title STRING NOT NULL,
    start_ms INT64 NOT NULL, -- Mapped from INT
    end_ms INT64 NOT NULL -- Mapped from INT
);

-- Junction table for Show Hosts (Many-to-Many relationship between SHOWS and PEOPLE)
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.SHOW_HOSTS` (
    showId STRING NOT NULL, -- Mapped from UUID
    personId STRING NOT NULL -- Mapped from UUID
    -- BigQuery does not enforce composite primary keys, but the combination
    -- of showId and personId is intended to be unique logically.
);

-- Junction table for Episode Guests (Many-to-Many relationship between EPISODES and PEOPLE)
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.EPISODE_GUESTS` (
    episodeId STRING NOT NULL, -- Mapped from UUID
    personId STRING NOT NULL -- Mapped from UUID
    -- BigQuery does not enforce composite primary keys, but the combination
    -- of episodeId and personId is intended to be unique logically.
);

-- New Table for REFERENCES
CREATE OR REPLACE TABLE `metapod-d52fc.dev02.REFERENCES` (
    id STRING NOT NULL, -- Primary Key, Mapped from UUID
    speaker_id STRING,  -- Foreign Key to PEOPLE.id, Mapped from UUID
    embedding ARRAY<FLOAT64>, -- List of floats
    embedding_type STRING,    -- Type of embedding (e.g., "speaker", "content")
    audioId STRING            -- Foreign Key to AUDIO.id, Mapped from UUID
);
