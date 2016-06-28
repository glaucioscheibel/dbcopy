BEGIN
    FOR c IN
        (SELECT c.owner,c.table_name,c.constraint_name
        FROM user_constraints c,user_tables t
        WHERE c.table_name=t.table_name
            AND c.status='ENABLED'
        ORDER BY c.constraint_type DESC,c.last_change DESC)
    LOOP
        FOR D IN
            (SELECT P.Table_Name Parent_Table,C1.Table_Name Child_Table,C1.Owner,P.Constraint_Name Parent_Constraint,
                c1.constraint_name Child_Constraint
             FROM user_constraints p
             JOIN user_constraints c1 ON(p.constraint_name=c1.r_constraint_name)
             WHERE(p.constraint_type='P'
                OR p.constraint_type='U')
                AND c1.constraint_type='R'
                AND p.table_name=UPPER(c.table_name))
        LOOP
            dbms_output.put_line('. Disable the constraint ' || d.Child_Constraint ||' (on table '||d.owner || '.' ||
            d.Child_Table || ')') ;
            dbms_utility.exec_ddl_statement('alter table ' || d.owner || '.' ||d.Child_Table || ' disable constraint ' ||
            d.Child_Constraint) ;
        END LOOP;
    END LOOP;
END;
