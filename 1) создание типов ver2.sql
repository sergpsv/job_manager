/*
CREATE OR REPLACE type tcloblist as table of clob
/
CREATE OR REPLACE type TStringlist as table of varchar2(500)
/
CREATE OR REPLACE type virtual_table_type as table of number
/



drop type TJob;
drop type treport;
drop type tmail;
drop type TFtpRecord;

*/

-- Start of DDL Script for Type DEPSTAT.TFTPRECORD
-- Generated 23/08/2010 8:59:49 from DEPSTAT@MKSTAT

CREATE OR REPLACE 
TYPE tftprecord
as object
(
  recipList       TstringList,
  filenames       TStringList,
  files           TClobList,
  IsArch          number,
  ArchPass        varchar2(50),
  member function GetOneRecip(/*self in out tftprecord,*/p_recip in varchar2, 
            p_fHost in out varchar2, p_fLogin in out varchar2, 
            p_fPass in out varchar2, p_fPath in out varchar2)return number,
  member procedure SetRecipients(p_recip in TStringList) ,
  member function SendReport return number,
  static function ftpCreate(p_filenames TstringList, p_files TclobList, p_isArch number) return TftpRecord
)
/


CREATE OR REPLACE 
TYPE BODY tftprecord
as
  static function ftpCreate(p_filenames TstringList, p_files TclobList, p_isArch number) return TftpRecord
  is
  begin
    return TftpRecord(null, p_filenames, p_files, p_isArch, null);
  end;

  -- получение одного хоста
  member function GetOneRecip(/*self in out tftprecord,*/p_recip in varchar2, 
            p_fHost in out varchar2, p_fLogin in out varchar2, 
            p_fPass in out varchar2, p_fPath in out varchar2) return number
  is
    l_str varchar2(500);
  begin
    if upper(p_recip) like 'FTP://%' then 
      l_str := substr(p_recip, 7);
    else
      l_str := p_recip;
    end if;
    -- 'FTP://dealer_upload:bKB8pTPc@10.23.10.234'
    p_fLogin := substr(l_str, 1, instr(l_str,':')-1);
    p_fPass  := substr(l_str, 1+instr(l_str,':') , instr(l_str,'@')-1-length(substr(l_str, 1, instr(l_str,':'))));
    if instr(l_str,'/') > 0 then 
        p_fhost  := substr(l_str, 1+instr(l_str,'@') , instr(l_str,'/')-1-length(substr(l_str, 1, instr(l_str,'@'))));
        p_fPath  := substr(l_str, 1+instr(l_str,'/') );
    else
        p_fhost  := substr(l_str, 1+instr(l_str,'@'));
        p_fPath  := '';
    end if;
    return 0;
  end;

  -- установка списка хостов
  member procedure SetRecipients(p_recip in TStringList)
  is
  begin
    if p_recip.count > 0 then 
      recipList := p_recip;
    else
      recipList := null;
    end if;
  end;
  
  -- отправка отчетов
  member function SendReport return number
  is
    l_conn  UTL_TCP.connection;
    l_b     blob;
    fhost   varchar2(255);
    fLogin  varchar2(255);
    fPass   varchar2(255);
    fPath   varchar2(500);
    l_syst  varchar2(100);
    f number;
  begin
    if (filenames is null) or (filenames.count=0) then return -1; end if;
    if (files is null) or (files.count=0) then return -1; end if;
    for i in filenames.first .. filenames.last
    loop
      for j in recipList.first .. recipList.last
      loop
        f:=GetOneRecip(/*self, */recipList(j), fHost, fLogin, fPass, fPath);
        dbms_output.put_line('flogin=('||flogin||') fpass=('||fpass||') fhost=('||fhost||') fpath=('||fpath||')');
        l_conn := ftp.login(fHost, '21', fLogin, fPass); 
        l_syst := ftp.getsystem(l_conn);
        if length(fpath)>0 then 
            if lower(l_syst) not like '%windows%' then 
              fpath := replace(fpath,'/','\')||'\';
            else
              fpath := fpath||'/';
            end if;
        end if;
        if IsArch = 1 then 
          l_b := pck_zip.clob_compress(files(i), filenames(i));
          ftp.put_remote_binary_data(l_conn, nvl(fpath,'')||substr(filenames(i),1,instr(filenames(i),'.'))||'zip', l_b);    
        else
          ftp.put_remote_ascii_data(l_conn, nvl(fpath,'')||filenames(i), files(i));    
        end if;
        ftp.logout(l_conn);
      end loop;
    end loop;
    return 0;
  end; 
end;
/


-- End of DDL Script for Type DEPSTAT.TFTPRECORD

-- Start of DDL Script for Type DEPSTAT.TJOB
-- Generated 23/08/2010 8:59:49 from DEPSTAT@MKSTAT

CREATE OR REPLACE 
TYPE tjob
as object
(
  job_id              number        ,
  job_name            varchar2(100),
  nextdat             DATE          ,
  nextformula         varchar2(1000)  ,
  maxnextdat          date          ,
  param1              VARCHAR2(100)  ,
  param2              VARCHAR2(100)  ,
  param3              VARCHAR2(100)  ,
  sendOnlyAnswerable  NUMBER        ,
  LastStart           date          ,
  run_by_job          NUMBER        ,
  maxParallel         number        ,
  job_comment         VARCHAR2(500) ,
  MAP MEMBER FUNCTION GetJobID RETURN NUMBER,
  member function GetNextDate  return date,
  -- get strvalue
  member function LastStart_str return varchar2,
  member function nextdat_str   return varchar2,
  member function GetRecipientList  return TStringList
)
/


CREATE OR REPLACE 
TYPE BODY tjob
as
  -- дл€ сортировок
  map member function GetJobID return number
  is
  begin
    return job_id;
  end GetJobID;

  member function GetNextDate  return date
  is
    dat date := null;
    l_interval varchar2(255) := replace(upper(nextformula),':NEXTDAT','to_date('''||to_char(nextdat,'dd.mm.yyyy hh24:mi:ss')||''',''dd.mm.yyyy hh24:mi:ss'')');
  begin
    IF nextformula IS NOT NULL THEN
      execute immediate 'select '||l_interval ||' from dual' into dat;
      if dat<nextdat then
        raise_application_Error(-20000, 'ƒата после вычислени€ интервала меньше прошлого запуска');
      end if;
    end if;
    return dat;
  end;

  member function LastStart_str return varchar2
  is
  begin
    return 
       case 
          when laststart is null then 'null' 
       else 
          'to_date('''||to_char(laststart,'dd.mm.yyyy hh24:mi:ss')||''',''dd.mm.yyyy hh24:mi:ss'')' 
    end;
  end;

  member function nextdat_str return varchar2
  is
  begin
    return 
       case 
          when nextdat is null then 'null' 
       else 
          'to_date('''||to_char(nextdat,'dd.mm.yyyy hh24:mi:ss')||''',''dd.mm.yyyy hh24:mi:ss'')' 
    end;
  end;

  member function GetRecipientList  return TStringList
  is
    ts  TStringList := TStringList();
--    job number := 2;
  begin
    if self.sendOnlyAnswerable = 1 then 
      for i in (select recipient from j_recipient j where j.job_id = self.job_id and answerable=1)
      loop
        ts.extend; ts(ts.count):=i.recipient; 
      end loop;
    else
      for i in (select recipient from j_recipient j where j.job_id = self.job_id and nvl(enabled,0)=1
              and nvl(MAXSENDDATE,to_date('31.12.2999'))>sysdate )
      loop
        ts.extend;
        ts(ts.count):=i.recipient; 
      end loop;
    end if;    
    if ts.count=0 then ts.extend; ts(1) := '<sergey.parshukov@megafonkavkav.ru>'; end if;
    /*for i in ts.first .. ts.last 
    loop
      dbms_output.put_line(ts(i));
    end loop; */
    return ts;
  end;
  
end;
-- End of DDL Script for Type DEPSTAT.TJOB

-- Start of DDL Script for Type DEPSTAT.TMAIL
-- Generated 21/04/2008 10:43:01 from DEPSTAT@VORONDB
/


-- End of DDL Script for Type DEPSTAT.TJOB

-- Start of DDL Script for Type DEPSTAT.TMAIL
-- Generated 23/08/2010 8:59:49 from DEPSTAT@MKSTAT

CREATE OR REPLACE 
type tmail
as object
(
  MailSubj        varchar2(255),
  MailBody        clob,
  Recipients      TStringList,
  attach_names    TStringList,
  attach_files    TClobList,
  IsArch          number,
  ArchPass        varchar2(50),
  member procedure SetRecipients(p_recip in TStringList) ,
  member function  SendReport return number,
  static function MailCreate( p_MailSubj        varchar2,
                              p_MailBody        clob,
                              p_attach_names    TStringList,
                              p_attach_files    TClobList
                              ) return TMail
)
/


CREATE OR REPLACE 
TYPE BODY tmail
as
-- конструктор
  static function MailCreate( p_MailSubj        varchar2,
                              p_MailBody        clob,
                              p_attach_names    TStringList,
                              p_attach_files    TClobList
                              ) return TMail
  is
  begin
    return TMAIL(p_MailSubj,    -- p_MailSubj
                 p_MailBody,    -- MailBody 
                 null,
                 p_attach_names,-- r_mail
                 p_attach_files,-- TFtpRecord 
                 0,             -- p_IsArch
                 null           -- p_ArchPass
                 );
  end MailCreate;
  
  member procedure SetRecipients(p_recip in TStringList) 
  is
  begin
    Recipients := p_recip;
  end;
  
  member function SendReport return number
  is
  begin
  
     if (attach_names is not null and attach_names.count>=1) and 
       (attach_files is not null and attach_files.count>=1) then 
      return emailsender.sendemailwithattach(              
              Recipients,
              mailSubj,
         	    mailBody,
  						attach_names, 
              attach_files,
              case when IsArch=1 then true else false end, 
              ArchPass
              );
    else
      return emailsender.sendemailwithattach(              
              Recipients,
              mailSubj,
         	    mailBody,
  						null, 
              null, null, null
              );
    end if;
  end; 
  
end;

-- End of DDL Script for Type DEPSTAT.TMAIL

-- Start of DDL Script for Type DEPSTAT.TREPORT
-- Generated 21/04/2008 10:43:01 from DEPSTAT@VORONDB
/


-- End of DDL Script for Type DEPSTAT.TMAIL

-- Start of DDL Script for Type DEPSTAT.TREPORT
-- Generated 23/08/2010 8:59:49 from DEPSTAT@MKSTAT

CREATE OR REPLACE 
TYPE treport 
as object
( 
  result          number,
  result_txt      varchar2(500),
  viaSystem       number,       -- 0=неопределена, 1=Email, 2=FTP
  r_mail          TMail,
  r_ftp           TFtpRecord,
  reportStart     date ,
  reportSend      date , 
  -- дл€ сортировок
  MAP MEMBER FUNCTION get_result RETURN NUMBER,
  -- конструктор
  static function ReportCreate(p_res in number, p_res_txt in varchar2) return TReport,
  -- установка списка получателей
  member procedure SetRecipients (p_recip in TStringList) ,
  -- отправка отчета через отчетную систему
  member function SendReport(p_reportSend in out date) return number
)
/


CREATE OR REPLACE 
TYPE BODY treport
as
  -- дл€ сортировок
  map member function get_result return number
  is
  begin
    return result;
  end get_result;

  -- конструктор
  static function ReportCreate(p_res in number, p_res_txt in varchar2) return TReport
  is
  begin
    return TReport(p_res, p_res_txt, 
                  0,
                  null,    -- r_mail
                  null    -- TFtpRecord 
                  /*null,null,null -- params */
                  ,null    -- reportStart
                  ,sysdate -- reportSend
                  );
  end ReportCreate;

  -- установка списка получателей  
  member procedure SetRecipients(p_recip in TStringList) 
  is 
    l_prefix varchar2(4);
  begin
    if p_recip is not null then
      if p_recip.count > 0 then 
        l_prefix := upper(substr(p_recip(1), 1, 4));
        viaSystem := case when l_prefix='FTP:' then 2 else 1 end;
        if (viaSystem = 1) and (r_mail is not null) then 
          r_mail.SetRecipients(p_recip);
        elsif (viaSystem = 2) and (r_ftp is not null) then 
          -- 'FTP://dealer_upload:bKB8pTPc@10.23.10.234'
          r_ftp.SetRecipients(p_recip);
        end if;
      else
        raise_application_Error(-20001,'—писок получателей пуст');
      end if;
    else
      raise_application_Error(-20001,'ѕередан пустой указатель на список получателей');
    end if;
  end;

  -- отправка отчета через отчетную систему
  member function SendReport(p_reportSend in out date) return number
  is
  begin
--    if viaSystem = 1 then 
      if r_mail is not null then
        p_reportSend := sysdate;
        return r_mail.sendReport;
      --else
      --  raise_application_Error(-20001,'TReport.viaSystem=1, но r_mail is null');
      end if;
  --  elsif (viaSystem = 2) then
      if r_ftp is not null then
        p_reportSend := sysdate;
        return r_ftp.sendReport;
      --else
      --  raise_application_Error(-20002,'TReport.viaSystem=2, но r_ftp is null');
      end if;
--    else
--      raise_application_Error(-20003, 'TReport.viaSystem not in (1,2) т.е. неопределена='||nvl(to_char(viaSystem),'null'));
--    end if;
  end SendReport;
  
end;-- End of DDL Script for Type DEPSTAT.TREPORT
/


-- End of DDL Script for Type DEPSTAT.TREPORT

