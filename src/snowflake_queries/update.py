# UPDATE
create_temporary_students_table = """MERGE INTO warehouse_students
    USING temporary_warehouse_students
ON warehouse_students.source_student_id = temporary_warehouse_students.source_student_id
    WHEN matched THEN
        update set warehouse_students.update_date = CURRENT_TIMESTAMP
    WHEN NOT matched THEN
        INSERT (source_student_id, name, surname, country, update_date, created_date)
            VALUES (temporary_warehouse_students.source_student_id,
            temporary_warehouse_students.name,
            temporary_warehouse_students.surname,
            temporary_warehouse_students.country,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
            ); """


create_temporary_exams_table = """MERGE INTO warehouse_exams
USING temporary_warehouse_exams
ON warehouse_exams.source_student_id = temporary_warehouse_exams.source_student_id
    WHEN matched THEN
        UPDATE SET warehouse_exams.update_date = CURRENT_TIMESTAMP
    WHEN NOT matched THEN
    INSERT (source_student_id, cohort_id, campus_id, teacher_id, update_date, created_date, score_value)
        VALUES (temporary_warehouse_exams.source_student_id,
        temporary_warehouse_exams.cohort_id,
        temporary_warehouse_exams.campus_id,
        temporary_warehouse_exams.teacher_id,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        temporary_warehouse_exams.score_value
        ); """