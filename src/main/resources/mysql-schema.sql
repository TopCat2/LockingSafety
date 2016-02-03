
DELIMITER ;
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


DELIMITER ;
DROP PROCEDURE if exists complete_file;

DELIMITER //
CREATE PROCEDURE complete_file (name varchar(255), out file_missing BOOLEAN)
begin
  UPDATE files_processed
     SET execution_status = "COMPLETED"
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
CREATE PROCEDURE claim_file (name varchar(255), out got_it BOOLEAN, out prev_status varchar(10))
begin


START TRANSACTION;


INSERT INTO files_processed (
file_name, execution_count, execution_status, begin_time,
	complete_time, run_node, run_pid)
VALUES (name, 0, "", NOW(),
	null, "petersPC", 123)
ON DUPLICATE KEY UPDATE
	execution_count = execution_count;


SELECT execution_status FROM files_processed
where file_name = name
into prev_status;

IF prev_status = "COMPLETED" THEN
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