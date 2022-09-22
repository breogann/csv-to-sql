# CREATE

create_warehouse_students = """
CREATE TABLE IF NOT EXISTS warehouse_students
(
    id                int IDENTITY(1,1) PRIMARY KEY,
    source_student_id varchar(255),
    name              varchar(255),
    surname           varchar(255),
    country           varchar(255),
    update_date       timestamp,
    created_date    timestamp
);
"""


create_warehouse_exams = """
CREATE TABLE IF NOT EXISTS warehouse_exams 
(
    id                int IDENTITY(1,1) PRIMARY KEY,
    source_student_id varchar(255),
    cohort_id         varchar(255),
    campus_id         varchar(255),
    teacher_id        varchar(255),
    update_date       timestamp,
    created_date    timestamp,
    score_value       int
);
"""

create_temporary_students_table = """MERGE INTO warehouse_students
    USING temporary_warehouse_students
ON warehouse_students.source_student_id = temporary_warehouse_students.source_student_id
    WHEN NOT matched THEN
        INSERT (source_student_id, name, surname, country)
            VALUES (temporary_warehouse_students.source_student_id,
            temporary_warehouse_students.name,
            temporary_warehouse_students.surname,
            temporary_warehouse_students.country); """


create_temporary_exams_table = """MERGE INTO warehouse_exams
USING temporary_warehouse_exams
ON warehouse_exams.source_student_id = temporary_warehouse_exams.source_student_id
    WHEN NOT matched THEN
    INSERT (source_student_id, cohort_id, campus_id, teacher_id, score_value)
        VALUES (temporary_warehouse_exams.source_student_id,
        temporary_warehouse_exams.cohort_id,
        temporary_warehouse_exams.campus_id,
        temporary_warehouse_exams.teacher_id,
        temporary_warehouse_exams.score_value
        ); """

view = """CREATE VIEW IF NOT EXISTS warehouse_progress AS 
        SELECT students.source_student_id, name, surname, country, score_value
            FROM warehouse_students as students
                JOIN warehouse_exams as exams 
                ON students.source_student_id=exams.source_student_id;
                """