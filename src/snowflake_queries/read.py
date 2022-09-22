# READ
last_created_table = """SELECT table_schema,
                           table_name,
                           created,
                           last_altered
                                FROM information_schema.tables
                                WHERE created > DATEADD(DAY, -30, CURRENT_TIMESTAMP)
                                      AND table_type = 'BASE TABLE'
                                ORDER BY created desc
                                LIMIT 1;"""