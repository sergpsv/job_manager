declare
  res    number        := 0;
  msg    varchar2(200) := '';
  dat    date          := sysdate;
  j      TJob          :=TJOB(0, 'test in manual mode', null,null,null,
                              null,null,null,
                              1,sysdate,0,0,'manual start');
  r      TReport;
begin
  log_ovart(1, '-', '');
  r := ; --job_procedures.volkova_calls(j, to_date('16.07.2008'));
  r.SetRecipients(TStringList('<sergey.parshukov@megafonkavkaz.ru>'));
  dbms_output.put_line(r.SendReport(dat));
dbms_output.put_line(res);
exception
  when others then
    dbms_output.put_line(SQLCODE ||' '||SQLERRM);
end;


