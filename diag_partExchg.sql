select  a.column_id COLUMN_ID, 
        a.table_name TABLE_NAME,
        a.column_name, 
        a.data_type, 
        a.data_length
from    dba_tab_columns a, 
        dba_tab_columns b
where   a.column_id (+) = b.column_id
and     (a.data_type != b.data_type or a.data_length != b.data_length)
and     a.table_name = upper('MVW2_3203')       and a.owner = 'SVNG'
and     b.table_name = upper('MVW2_INT_3203')  and b.owner = 'SVNG'
union
select  b.column_id COLUMN_ID, 
        b.table_name TABLE_NAME,
        b.column_name, b.data_type, b.data_length
from    dba_tab_columns a, 
        dba_tab_columns b
where   b.column_id (+) = a.column_id 
and     (a.data_type != b.data_type or a.data_length != b.data_length)
and     a.table_name = upper('MVW2_3203')       and a.owner = 'SVNG'
and     b.table_name = upper('MVW2_INT_3203')  and b.owner = 'SVNG'
order by table_name, column_id;


(
select  'in MVW2_3203' as loc ,
        column_name, 
        data_type, 
        data_length, 
        data_precision, 
        data_scale,
        SVNG.get_default(table_name, column_name) as dflt,
        nullable, 
        CHAR_COL_DECL_LENGTH, 
        CHAR_LENGTH, 
        decode(CHAR_USED, 'B', 'BYTES', 'C', '' ) C
from    dba_tab_columns
where   table_name = upper('MVW2_3203') 
and     owner = 'SVNG'
MINUS
select  'in MVW2_3203' as loc, 
        column_name, 
        data_type, 
        data_length, 
        data_precision, 
        data_scale,
        SVNG.get_default(table_name, column_name) as dflt ,
        nullable, 
        CHAR_COL_DECL_LENGTH, 
        CHAR_LENGTH, 
        decode(CHAR_USED, 'B', 'BYTES', 'C', '' ) C
from    dba_tab_columns
where   table_name = upper('MVW2_INT_3203') 
and     owner = 'SVNG'
)
UNION ALL
(
select  'in MVW2_INT_3203' as loc, 
        column_name, 
        data_type, 
        data_length, 
        data_precision, 
        data_scale,
        SVNG.get_default(table_name, column_name) as dflt ,
        nullable, 
        CHAR_COL_DECL_LENGTH, 
        CHAR_LENGTH, 
        decode(CHAR_USED, 'B', 'BYTES', 'C', '' ) C
from    dba_tab_columns
where   table_name = upper('MVW2_INT_3203') 
and     owner = 'SVNG'
MINUS
select  'in MVW2_INT_3203' as loc, 
        column_name, 
        data_type, 
        data_length, 
        data_precision, 
        data_scale,
        SVNG.get_default(table_name, column_name) as dflt,
        nullable, 
        CHAR_COL_DECL_LENGTH, 
        CHAR_LENGTH, 
        decode(CHAR_USED, 'B', 'BYTES', 'C', '' ) C
from    dba_tab_columns
where   table_name = upper('MVW2_3203')  
and     owner = 'SVNG'
) ;


(
select  'in MVW2_3203', 
        constraint_type, 
        status, 
        deferrable, 
        deferred, 
        validated
from    dba_constraints
where   table_name = upper('MVW2_3203') 
and     owner = 'SVNG'
MINUS
select  'in MVW2_3203', 
        constraint_type, 
        status, 
        deferrable, 
        deferred, 
        validated
from    dba_constraints
where   table_name = upper('MVW2_INT_3203') 
and     owner = 'SVNG'
)
UNION ALL
(
select  'in MVW2_INT_3203', 
        constraint_type, 
        status, 
        deferrable, 
        deferred, 
        validated
from    dba_constraints
where   table_name = upper('MVW2_INT_3203')  
and     owner = 'SVNG'
MINUS
select  'in MVW2_INT_3203', 
        constraint_type, 
        status, 
        deferrable, 
        deferred, 
        validated
from    dba_constraints
where   table_name = upper('MVW2_3203')  
and     owner = 'SVNG'
);
