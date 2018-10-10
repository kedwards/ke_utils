--
-- Required type
--
create or replace type o_column_value_difference is object (
    column_name varchar2(2000)
    , data_value_pair varchar2(4000)
    , first_value varchar2(2000)
    , second_value varchar2(2000)
);
/
show errors

create or replace type c_column_value_difference is table of o_column_value_difference;
/
show errors

create or replace package ke_utils
is
--
-- key_value_pair_p must look like this  emp_id=1:emp_ver_no=1|emp_id=1:emp_ver_no=2
-- the routine will change single quotes to double quotes for you
-- and add the WHERE and AND components to finish up a set of where clauses
-- please note we are using the pipe (|) and colon (:) as delimiters
-- so if you are using date formats make sure not to use either in your format string
-- it is expected that each expression yeilds one row thus you should be supplying primary or unique key data
-- maybe some day I'll make this into a generice two where clause deal
--
-- select a.#pk_cols#, b.*
-- from (
--   select lag(rowid) over (partition by #pk_cols# order by #pk_cols#) a_rowid
--     , rowid b_rowid
--     , #table#.*
--   from #table#
--   --where #pk_cols# = 'pk_values'
-- ) a, table(cast(ke_utils.show_column_diffs(
--     user, upper('#table#'), a_rowid, b_rowid, '', 0, 1) as c_column_value_difference
-- )) b
-- order by 1,2
-- ;
--
    function show_column_diffs (
        owner_p in varchar2
        , table_name_p in varchar2
        , a_rowid_p in rowid
        , b_rowid_p in rowid
        , excluded_columns_list_p in varchar2
        , match_type_p in number default 0
        , return_type_p in number default 1
    ) return c_column_value_difference;
end;
/
show errors

create or replace package body ke_utils
is
    function show_column_diffs (
         owner_p in varchar2
         , table_name_p in varchar2
         , key_value_pair_p in varchar2
         , excluded_columns_list_p in varchar2
         , match_type_p in number
         , return_type_p in number
     ) return c_column_value_difference
     ;
     
     function show_column_diffs (
        owner_p in varchar2
        , table_name_p in varchar2
        , a_rowid_p in rowid
        , b_rowid_p in rowid
        , excluded_columns_list_p in varchar2
        , match_type_p in number default 0
        , return_type_p in number default 1
    ) return c_column_value_difference is
    begin
        return show_column_diffs (
            owner_p
            , table_name_p
            , 'rowid='''||a_rowid_p||'''|rowid='''||b_rowid_p||''''
            , excluded_columns_list_p
            , match_type_p
            , return_type_p
        );
    end;

    function show_column_diffs (
         owner_p in varchar2
         , table_name_p in varchar2
         , key_value_pair_p in varchar2
         , excluded_columns_list_p in varchar2
         , match_type_p in number
         , return_type_p in number
     ) return c_column_value_difference
     is
         where_string_1_v varchar2(4000);
         where_string_2_v varchar2(4000);
         sql_v varchar2(32000);
         col_expression_v varchar2(32000);
         c_column_value_difference_v c_column_value_difference := c_column_value_difference();
         c_column_value_difference_f_v c_column_value_difference := c_column_value_difference();
     begin
         where_string_1_v := substr(key_value_pair_p,1,instr(key_value_pair_p,'|')-1);
         where_string_1_v := 'a.'||replace(where_string_1_v,':',' and a.');
         where_string_2_v := substr(key_value_pair_p,instr(key_value_pair_p,'|')+1);
         where_string_2_v := 'b.'||replace(where_string_2_v,':',' and b.');

         for i in 1..50 loop
             sql_v := null;
             for r1 in (
                 select column_name,data_type
                 from all_tab_cols -- from dba_tab_columns
                 where table_name = table_name_p -- where owner = owner_p
                     and data_type in ('DATE','NUMBER','VARCHAR2','CHAR')
                     and instr(','||upper(excluded_columns_list_p)||',',','||column_name||',') = 0
                     and column_id between (i-1)*10+1 and (i*10)
                 order by column_name
                 )
                 loop
                    if return_type_p = 0 then
                         if r1.data_type in ('NUMBER','VARCHAR2','CHAR') then null;
                             col_expression_v := ''''||r1.column_name||''''||' column_name, substr(a.'||lower(r1.column_name)||'||'' / ''||b.'||lower(r1.column_name)||',1,4000) data_value_pair, null first_value, null second_value';
                         else
                             col_expression_v := ''''||r1.column_name||''''||' column_name,substr(nvl(to_char(a.'||lower(r1.column_name)||',''dd-mon-rrrr hh24:mi:ss''),lpad('' '',20,'' ''))||''/''||nvl(to_char(b.'||lower(r1.column_name)||',''dd-mon-rrrr hh24:mi:ss''),lpad('' '',20,'' '')),1,4000) data_value_pair, null first_value, null second_value';
                         end if;
                    else
                        if r1.data_type in ('NUMBER','VARCHAR2','CHAR') then null;
                            col_expression_v := ''''||r1.column_name||''''||' column_name, null data_value_pair, a.'||lower(r1.column_name)||' first_value, b.'||lower(r1.column_name)||' second_value';
                        else
                            col_expression_v := ''''||r1.column_name||''''||' column_name, null data_value_pair, nvl(to_char(a.'||lower(r1.column_name)||',''dd-mon-rrrr hh24:mi:ss''),lpad('' '',20,'' '')) first_value, nvl(to_char(b.'||lower(r1.column_name)||',''dd-mon-rrrr hh24:mi:ss''),lpad('' '',20,'' '')) second_value';
                        end if;
                    end if;

                     if match_type_p = 1 then
                        sql_v := sql_v||' union all select '||col_expression_v||' from '||owner_p||'.'||table_name_p||' a,'||owner_p||'.'||table_name_p||' b where '||where_string_1_v||' and '||where_string_2_v||' and decode(a.'||r1.column_name||',b.'||r1.column_name||',0,1) = 1';
                     else                        
                        sql_v := sql_v||' union all select '||col_expression_v||' from '||owner_p||'.'||table_name_p||' a,'||owner_p||'.'||table_name_p||' b where '||where_string_1_v||' and '||where_string_2_v||'' ;
                    end if;
                 end loop;

                 if sql_v is not null then
                     sql_v := 'select cast(multiset(select * from ( '||substr(sql_v,11)||' )) as c_column_value_difference ) from dual';
                     execute immediate sql_v into c_column_value_difference_v;
                     select cast(multiset(select * from(
                         select *
                         from table(cast(c_column_value_difference_v as c_column_value_difference))
                     union all
                         select *
                         from table(cast(c_column_value_difference_f_v as c_column_value_difference))
                     )) as c_column_value_difference)
                     into c_column_value_difference_f_v
                     from dual;
                 end if;
         end loop;
         if c_column_value_difference_f_v.last is null then
             c_column_value_difference_f_v.extend;
             c_column_value_difference_f_v(c_column_value_difference_f_v.last) := o_column_value_difference(null,null,null,null);
         end if;
         return (c_column_value_difference_f_v);
    end;
end ke_utils;
/
show errors

select a.employee_id, b.*
from (
  select lag(rowid) over (partition by employee_id order by employee_id) a_rowid
    , rowid b_rowid
    , val_hcm_hire_employees_diff.*
  from val_hcm_hire_employees_diff
  where employee_id = '200137'
) a, table(cast(ke_utils.show_column_diffs(
    user, upper('val_hcm_hire_employees_diff'), a_rowid, b_rowid, '', 0, 1) as c_column_value_difference
)) b
order by 1,2
;
