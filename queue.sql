-- /*
-- * - Keep a ordered list of `(seq#, timestamp, blob)` with all future work.
-- * - Keep a list of named pointers to `queue`
-- *
-- * For each concrete observer (this would most likely only be a single one) a pointer
-- * to the queue exists. History may be purged by selecting all `queue` items where `id`
-- * is less than the lowesdt `queue_id` in `pointer`.
-- *
-- * It's not allowed to delete a `queue` item with an active pointer.
-- */
;
CREATE TABLE `queue`
(
	`key` blob PRIMARY KEY NOT NULL,
	`timestamp` integer NOT NULL,
	`blob` blob  NOT NULL
);