DROP TABLE IF EXISTS files_processed;

CREATE TABLE `files_processed` (
  `file_name` varchar(128) NOT NULL,
  `execution_count` int(11) NOT NULL,
  `execution_status` varchar(9) NOT NULL,
  `begin_time` datetime NOT NULL,
  `complete_time` datetime DEFAULT NULL,
  `records` int(11) NULL,
  PRIMARY KEY (`file_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



DELIMITER ;

--  Valid execution_status strings and their intended meanings
--  RUNNING - a process has started with the file and is either still
--            running or has been terminated.
--  COMPLETED - the file has processed successfully and should not be run again.
--  FAILED - previous processing of the file generated an unexpected error. the
--           file can be rerun
--  REJECTED - the file failed validation and needs to be resubmitted.  a file
--             by the same name can be run again
--  CANCELED - condition set manually to indicate that the file should not be
--             allowed to run again even though it did not complete.
--  NEW - temporary state for "zeroth" iteration

DROP PROCEDURE if exists fail_file;

DELIMITER //
CREATE PROCEDURE fail_file (name varchar(255), out file_missing BOOLEAN)
begin
  UPDATE files_processed
     SET execution_status = "FAILED"
   WHERE file_name = name;
  SET @rows = ROW_COUNT();
  if (@rows < 1)
  THEN
	SET file_missing = TRUE;
  ELSE
    SET file_missing = FALSE;
  END IF;

end
//

DELIMITER ;

DROP PROCEDURE if exists complete_file;

DELIMITER //
CREATE PROCEDURE complete_file (name varchar(255), recs int, out file_missing BOOLEAN)
begin
  UPDATE files_processed
     SET execution_status = "COMPLETED", records = recs
   WHERE file_name = name;
  SET @rows = ROW_COUNT();
  if (@rows < 1)
  THEN
	SET file_missing = TRUE;
  ELSE
    SET file_missing = FALSE;
  END IF;

end
//

DELIMITER ;


DROP PROCEDURE IF EXISTS reject_file;
DELIMITER //
CREATE PROCEDURE reject_file (name varchar(255), out file_missing BOOLEAN)
begin
  UPDATE files_processed
     SET execution_status = "REJECTED"
   WHERE file_name = name;
  SET @rows = ROW_COUNT();
  if (@rows < 1)
  THEN
	SET file_missing = TRUE;
  ELSE
    SET file_missing = FALSE;
  END IF;

end
//

DELIMITER ;

DROP PROCEDURE if exists claim_file;
DROP FUNCTION if exists claim_file;


DELIMITER //
CREATE PROCEDURE claim_file (name varchar(255), out got_it BOOLEAN, out prev_status varchar(10), out prev_tries int)
begin

START TRANSACTION;

INSERT INTO files_processed (
file_name, execution_count, execution_status, begin_time, complete_time)
VALUES (name, 0, "NEW", NOW(),null)
ON DUPLICATE KEY UPDATE
	execution_count = execution_count;

SELECT execution_status, execution_count FROM files_processed
where file_name = name
into prev_status, prev_tries;

IF prev_status IN ("COMPLETED", "CANCELED") THEN
  ROLLBACK;
  SET got_it = FALSE;
ELSE
begin
  UPDATE files_processed
	SET begin_time = now(), execution_count = execution_count + 1, execution_status = "RUNNING"
  WHERE file_name = name;
  COMMIT;
  SET got_it = TRUE;
end;
END IF;

end
//

DELIMITER ;