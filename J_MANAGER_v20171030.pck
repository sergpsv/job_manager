CREATE OR REPLACE PACKAGE J_MANAGER IS
/*------------------------------------------------------------------------------  
  
  author = sparshukov
  
  ver 1: базовая функциональность
  ver 2: добавлена возможность запуска на тестовых серверах
  вер 3:
        SendResult   : l_only := nvl(p_job.sendonlyanswerable,0);
        SetJobAfter  : if nvl(p_job.nextdat,sysdate)>sysdate then l_ndat := p_job.nextdat; end if;
                     : 
        all text     : if p_job.job_id not in (id_ClearDiedJobs, id_RunOnTestServ) then
     dispatch_reports: and MAXNEXTDAT > nextdat;
     убрал метод GetNextDate
  
  вер 4:
    1) добавить группы в J_RECIPIENT
    2) Допускать отпрвку на адреса разных типов SMTP, FTP, WebDAV
    3) при отправке только ответственному в письмо вкладывать адреса тех, кто получил бы нормальном режиме
    4) в случае сбоя отправки по почте - отправлять на FTP
    5) не работает вызов из среды SetRecipient and SendReport (при тестировании)

2011/09/01 - добавлено использование контекста
-------
2011/11/09 - внесены проверки на IP адреса, с которых работает диспетчер и уведомление о том, что ЕМАЙЛ может не отправляться!!
             добавлена функция информирования INFORM.
             добавлено оповещение о переходе в SUSPEND=TRUE, ХОСТ НЕ ЯВЛЯЕТСЯ РАЗРЕШЕННЫМ
2011/11/10 - по другому вычисляет дату запуска при использовании SCHEDULER (с использованием переменной типа INTERVAL)
2011/11/14 - оставляет первую строку в истории J_LIST_HISTORY. Можно понять когда впервые начал работать джоб
-------
2011/12/06 - ClearDiedJobs внес в тело джоба dispatch_reports
-------
2012/01/05 - более корректное определение автора
2012/01/06 - расширен функционал DEBUGMSG - сохранение копии сообщения на основной сервер
           - в SendResult исправлен код смс информирования. В случае пустого отчета смс ранее не приходила
2012/01/12 - добавил метод GetAuthor и вынес в него код из SetJobBefore
-------
2012/04/10 - добавил таблицу параметров J_LIST_ADD_PARAM
                + Общее кол-во запусков
                + Кол-во неуспешных запусков
                + Кол-во отложенных стартов
                + LAST_KICK 
                + LAST_KICK_from
           - В письмо в подпись добавить емайл ответственного для обратной связи 
           - В историю результатов джоба - сервер
2012/04/28 - обернул utl_inaddr.get_host_address в exception
-------
2012/05/04 - включил DropAnyTempTabInThisSess в setJobAfter для удаления тестовых таблиц после работы джоба
             тестовыми признаются все таблицы с определнной маской(настраивается) и содержажие в комментариях к таблице SID текущей сессии
             удалени вешается на джоб через Х часов.
2012/05/10 - добавлена спецификация Reply-to в SetJobBefore на основе информации об ответственном
2012/05/12 - исправелны баги в GetManagerParam[x]. Функция могла завершится без возврата значения. Теперь возвращает NULL.
           - в процедуре протоколирования, раздел записи на боевую базу могла произойти ошибка, если не было дб-линка
2012/05/15 - в SetJob_Before добавил занесение в контекст некоторых параметров джоба
-------
2012/06/27 - извещаем всех заинтересованных лиц, что поднялся диспетчер (по таблице вместо конкретных номеров)
-------
2012/07/23 - Добавил GetRecipientList - для получения данных о списке получателей
-------
2012/10/15 - в KickJob добавил перехват исключений
           - а Addp_set новый параметр - дблинк на базу в которой обновляется параметр 
           - к имени файла Error_protokol добавил номер джоба 
--------
2012/11/09 - ParseAndTestJobName теперь возвращает тип значения, которое возвращает функция  

--------
2013.02.20 - масштабный переход к TAdvReport

--------
2013.03.19 - RunJobSaveResult изменение набора параметров для фукнции
           - GetJobParams - другой подход к формированию списка паметров            
---------
2013.05.22  - RunJobSaveResult - передача RUN_BY_JOB=0

---------
2013.10.02 - добавил alter_session
           - переписал set_before и заодно исправил баг, при ктоорм не выставлялось использование QUERY REWRITE
             если у джоба не было нормального описания этой опции в j_list_add_param

---------
2014.08.26  - RunJobSaveResult - добавил сброс локальной даты следующего старта, если кикнули снаружи и завершился res_error\res_continue
            - dispatch_reports - при запуске с мастера, ведомая копия расчитывает дату следующего старта и апдейтит ее на мастер и копию.
                               - ввел проверку upper(p_server_kind) <> 'MAIN' and l_master_cronss=1 and abs(l_master_nextD-i.nextdat) < 0.5/24 

  подсчет кол-ва переносов по RES_CONTINUE - и не спамить письмами о сдвиге запуска
  ------------------------------------------------------------------------------*/
  res_success  number := 1;
  res_continue number := 0;
  res_error    number := -1;

  TYPE TParamList is table of j_dispatcher.paramvalue%type index by j_dispatcher.paramNAME%type;
  ParamList TparamList;

  l_debug      boolean := false;
  l_debugLevel number := 5;
      -- 0: информационные сообщения общего характера
      -- 1: более подробные сообщения по ходу выполнения
      -- 2: Вход в определенные функции и значения параметров при входе в них

  --------------------------------------------------------------------------------  
  function GetManagerParamV(p in varchar2) return varchar2;
  --------------------------------------------------------------------------------  
  procedure dispatch_reports(nd            in out date,
                             p_server_kind in varchar2 default 'main',
                             p_server_num  in number default 0);
  procedure SetJobBefore(p_job in out TJob);
--  procedure setJobAfter(p_job in out TJob, p_report in out TReport);
  procedure setJobAfter(p_job in out TJob, p_result in number, p_result_txt in varchar2);
  procedure SendResult(p_job in out TJob, p_report in out TReport);
  procedure SendResult(p_job in out TJob, p_report in out TADVReport);

  function ClearDiedJobs(p_job in out TJob) return TReport;
  FUNCTION RunJobOnTestServ(p_job in out TJOb) RETURN TReport;
  procedure RunJobSaveResult(p_job in out TJOb, res number, res_txt varchar2);
  function GetJobParams(p_job_id number) return varchar2;
  function GetAnswerAble(p_job_id number) return varchar2;

  procedure push_sch_jobs;
  function TestLinkAndServer(p_job in out TJob, p_link varchar2) return boolean;
  procedure ctx_set(field in varchar2, value in varchar2, cl_ident varchar2 default null, usr varchar2 default user);
  function kick_job(p_job_id in number, p_server in varchar2 default null, p_param1 in varchar default null, p_param2 in varchar default null, p_param3 in varchar2 default null, p_new_start date default sysdate+1/60/24) return number;
  function GetAuthor(p_job_name in varchar2)return varchar2;

  function  addp_get(p_job_id in number, p_part in varchar2 default 'default', p_paramname in varchar) return varchar;
  procedure addp_increment(p_job_id in number, p_part in varchar2 default 'default', 
    p_paramname in varchar, p_increment in number);
  function GetMsisdnByRecip(p_recip varchar2) return varchar2;
  procedure SendSms( p_msisdn_or_user varchar2 default user, p_text varchar2);
  function GetManagerParamB(p in varchar2) return boolean;
  procedure DropAnyTempTabInThisSess(p_table_mask in varchar2 default 'DROP%', p_sid_mask in varchar default sys_context('userenv','SID')||'-'||sys_context('userenv','SESSIONID'));
  function GetRecipientList(p_job_id number, p_propocol varchar2 default null) return TStringList pipelined;
  procedure addp_set(p_job_id in number, p_part in varchar2 default 'default', 
    p_paramname in varchar, p_paramvalue in varchar, p_comm varchar default null);
  function ParseAndTestJobName(p_job_name in varchar, 
  sch_name out varchar2, obj_name out varchar2, prc_name out varchar2, obj_type out varchar2,
  return_TYPE out varchar2) return number;
  function GetLastStart(p_job number, p_status number default res_success) return date;
  function getStatServNum return number;
  procedure alter_session(p_job_id number, p_param varchar2, p_value varchar2, p_logging number default 0);
  function HasFreshMKS(p_needDate date default sysdate, p_TestServNum number default 1) return number;
END; -- Package spec
/
CREATE OR REPLACE PACKAGE BODY J_MANAGER
IS
  id_ClearDiedJobs number       := 0;
  id_RunOnTestServ number       := 100;
  g_subj           varchar2(20) := sys_context('UserEnv','INSTANCE_NAME')||':';
  gpv_myEmail      varchar2(50) := 'Sergey.parshukov@megafon.ru';
  gpv_myFTP        varchar2(50) := 'FTP://ftpuser:ftp21gfcc!@10.23.23.175';
  gpv_myMsisdn     varchar2(50) := '79282011384';
  gpv_MsisdnTaranenko     varchar2(50) := '79282019951';
  
  type TRefCursor is ref cursor;

  l_dispValue      varchar2(100) := '';  
  l_tmp            varchar2(100) := ''; 
  l_dblMain        varchar2(100) := '@tobis_dp'; 
  
-------------------------------------------------------
FUNCTION IsStatServ return number;
-------------------------------------------------------
procedure ctx_set(field in varchar2, value in varchar2, cl_ident varchar2 default null, usr varchar2 default user) 
as
begin 
  dbms_session.set_context('jm_ctx', field, value, usr, cl_ident);
end;
  
-------------------------------------------------------
FUNCTION IsStatServ return number
is
  res number;
begin
  select count(*) into res from user_tables where table_name='TESTSERVER';
  return res;
end; 

--------------------------------------------------------------------------------------------------------
function Date_to_str(p_dat in date) return varchar2
is
begin
   return 
       case 
          when p_dat is null then 'null' 
       else 
          'to_date('''||to_char(p_dat,'dd.mm.yyyy hh24:mi:ss')||''',''dd.mm.yyyy hh24:mi:ss'')' 
   end;
end;

--------------------------------------------------------------------------------------------------------
procedure debugmsg(p_detailLevel in number default 0, p_part in  varchar2, p_Event in varchar2, p_job_num in number)
is
  PRAGMA AUTONOMOUS_TRANSACTION;
  l_event       varchar2(1500) := '';
  writeRowCount number := 0;
  l_db_on_host  varchar2(200):=sys_context('USERENV','DB_NAME')||' ('||sys_context('USERENV','DB_UNIQUE_NAME')||') on '||sys_context('USERENV','SERVER_HOST');
  j TJob   := TJob(p_job_num,'ххх',
                  to_date('05.09.2009 16:00:00','dd.mm.yyyy hh24:mi:ss') /*nextdat*/,
                  'trunc(add_months(sysdate,1),''month'')+4+16/24' /*NEXTFORMULA*/,
                  to_date('31.12.2999 00:00:00','dd.mm.yyyy hh24:mi:ss') /*MaxNextdat*/,
                  'trunc(sysdate,''month'')-1/86400' /* param1 */,
                  null /* param2 */,
                  null /* param3 */,
                  0    /*sendOnlyAnswerable*/,
                  sysdate /*laststart*/,
                  null    /*run_by_job*/,
                  0    /* maxparallel*/,
                  0);
begin
  if p_job_num = id_RunOnTestServ then commit; return; end if;
  if (l_debug and p_detailLevel<=l_debugLevel) or true then
    l_Event := nvl(LPAD(' ',abs(p_detailLevel*2))||p_Event, ' ');
    while (length(l_Event)/100) > writeRowCount 
    loop
      insert into j_log(iDebugLevel, cMsg, part, job_id, db_on_host )
      values(p_detailLevel , substr(l_Event, writeRowCount*100+1, 100), p_part, p_job_num, l_db_on_host);
      writeRowCount := writeRowCount +1;
    end loop;
    commit;
    if IsStatServ=1 and TestLinkAndServer(j, 'dblink_to_main') then 
      writeRowCount := 0;
      while (length(l_Event)/100) > writeRowCount 
      loop
        execute immediate 'insert into j_log'||l_dblMain||'(iDebugLevel, cMsg, part, job_id, db_on_host )
          values(:sl_detailLevel , :sl_Event, :sl_part, :sl_job_num, :l_dbh
                 )'
          using p_detailLevel , substr(l_Event, writeRowCount*100+1, 100), p_part, p_job_num, l_db_on_host;
        writeRowCount := writeRowCount +1;
      end loop;
    end if;
    if lower(substr(p_Event,1,9)) = 'exception' then 
      begin
         emailsender.sendto(GetAnswerAble(p_job_num), 'Ошибка:'||g_subj||' '||p_part, p_Event);
      exception 
        when others then
          emailsender.sendto(gpv_myEmail, 'Ошибка:'||g_subj||' '||p_part, p_Event);
      end;
    end if;
  commit;
  end if;
end;


--------------------------------------------------------------------------------
function ParseAndTestJobName(p_job_name in varchar, 
  sch_name out varchar2, obj_name out varchar2, prc_name out varchar2, obj_type out varchar2,
  return_TYPE out varchar2) return number
is
--  job_name      varchar2(100):='sparshukov.grant_priv'; -- ERROR
--  job_name      varchar2(100):='sparshukov.sph_subs.make_day'; --OK
--  job_name      varchar2(100):='j_manager.dispatch_reports'; -- OK
--  job_name      varchar2(100):='log_ovart'; -- OK
  l_job_name    varchar2(200):=trim(p_job_name);
  l_dot_cnt     number  :=0;
  l_exists      number  :=0;
  l_str         varchar2(200);
  
  l_line_start number;
  l_line_end   number;
  l_strarray   varchar2(2000):='';
  
/*  sch_name   varchar2(100):='';
  obj_name   varchar2(100):='';
  prc_name   varchar2(100);*/
begin
  for i in 1..length(p_job_name)
  loop if substr(p_job_name,i,1)='.' then l_dot_cnt:=l_dot_cnt+1; end if; end loop;
--  dbms_output.put_line('кол-во точек= '||l_dot_cnt);
  if l_dot_cnt=0 then 
     sch_name:=user; 
     obj_name:=upper(p_job_name);
  elsif l_dot_cnt=1 then 
     sch_name:=user; 
     obj_name:= upper(substr(l_job_name,1,instr(l_job_name,'.')-1)); 
     prc_name:= upper(substr(l_job_name,  instr(l_job_name,'.',-1,1)+1));
  elsif l_dot_cnt=2 then 
     sch_name:= upper(substr(l_job_name,1,instr(l_job_name,'.')-1)); l_job_name:= substr(l_job_name,  instr(l_job_name,'.')+1);
     obj_name:= upper(substr(l_job_name,1,instr(l_job_name,'.')-1)); 
     prc_name:= upper(substr(l_job_name,  instr(l_job_name,'.',-1,1)+1));
  end if;
  select count(1), max(object_type) into l_exists, obj_type  from all_procedures 
  where owner=sch_name and OBJECT_NAME = obj_name and nvl(PROCEDURE_NAME,'a')=nvl(prc_name, 'a') and nvl(overload,1)=1;
  
  select max(case when DATA_TYPE='OBJECT' then TYPE_OWNER||'.'||TYPE_NAME 
            when DATA_TYPE='PL/SQL RECORD' then TYPE_NAME||'.'||TYPE_SUBNAME
            else PLS_TYPE end) into return_TYPE
  from all_arguments a 
  where owner=sch_name and POSITION=0
  and (PACKAGE_NAME=obj_name or PACKAGE_NAME is null)
  and object_name = nvl(prc_name,obj_name);

--  dbms_output.put_line('1) l_exists='||l_exists||',obj_type='||obj_type||',l_dot_cnt'||l_dot_cnt);
  if l_exists = 0 then 
    if l_dot_cnt=1 then 
      select count(1) into l_dot_cnt from all_users where username = obj_name;
--      dbms_output.put_line('2) l_dot_cnt'||l_dot_cnt);
      if l_dot_cnt = 1 then 
        sch_name:=obj_name; 
        obj_name:= prc_name;
        prc_name:= '';
        select count(1), max(object_type) into l_exists, obj_type from all_procedures 
        where owner=sch_name and OBJECT_NAME = obj_name and nvl(overload,1)=1;
      else -- возможно у нас не публичный метод в пакете (анализируем, кому принадледит пакет)
        -- если пакет существует - вернем -1
        select 0-count(distinct sch_name), 'PACKAGE BODY', min(line) into l_exists, obj_type, l_line_start
        from all_source where owner=sch_name and name = obj_name and type = 'PACKAGE BODY' 
        and (lower(text) like '%procedure%'||lower(prc_name)||'%' or lower(text) like '%function%'||lower(prc_name)||'%');

        -- теперь попытаемся найти тип 
        select min(line) into l_line_end 
        from all_source where owner=sch_name and name = obj_name and type = 'PACKAGE BODY' 
        and line >= l_line_start
        and ( lower(text)='is' or lower(text) like 'is %' or lower(text) like '% is' or lower(text) like '% is %' 
              or lower(text) like 'is'||chr(9)||'%' or lower(text) like '%'||chr(9)||'is' or lower(text) like '%'||chr(9)||'is'||chr(9)||'%');
        for i in (select text from all_source where owner=sch_name and name = obj_name and type = 'PACKAGE BODY' 
                  and line between l_line_start and l_line_end)
        loop
          if (length(l_strarray)+length(i.text)> 2000) then exit; end if;
          l_strarray := l_strarray || upper(i.text);
        end loop;
        
        dbms_output.put_line('l_strarray='||l_strarray);
        return_TYPE := trim(regexp_substr(l_strarray,'[)](return)?(.+[^IS])',1,1,'i',2));
      end if;
    end if;
  end if;

  return l_exists;
end;



--------------------------------------------------------------------------------
function GetLastStart(p_job number, p_status number default res_success) return date
is
  l_part varchar2(100) := 'GetLastStart';
  l_res date;
  l_j   number;
begin
  select count(1) into l_j from j_list where job_id = p_job;
  if l_j=0 then 
    debugmsg(0,l_part,'job_id = '||p_job||' не найден в списке джобов',0);
  else
    select max(LASTSTART) into l_res from j_list_history where job_id = p_job and jobres=p_status;
  end if;
  return l_res;
end;

--------------------------------------------------------------------------------
function GetStartDate return date
is
  dat     date   := sysdate;
  l_cnt   number ;
  l_found number := 0;
begin
   begin
      while l_found=0 and dat > (sysdate - 32)
      loop
        execute immediate 'select count(1) from calls_00_'||to_char(sysdate,'mmyyyy')||' partition (calls_'||to_char(dat,'yyyymmdd')||') where rownum<=10000' into l_cnt;
        if l_cnt=10000 then 
          execute immediate 'select max(insert_date) from calls_00_'||to_char(sysdate,'mmyyyy')||' partition (calls_'||to_char(dat,'yyyymmdd')||')' into dat;
          l_found:=1;
        else
          dat := dat-1;
        end if;
      end loop;
  exception 
    when others then 
       select STARTUP_TIME into dat from v$instance;
  end;  
  return dat;
end;

--------------------------------------------------------------------------------
function GetJobParams(p_job_id number) return varchar2
is
  l_param varchar2(1000) := '';
  p1  j_list.param1%type;
  p2  j_list.param2%type;
  p3  j_list.param3%type;
  l_part  j_log.part%type := 'GetJobParams';
begin
    select param1, param2, param3 into p1, p2, p3 from j_list where job_id=p_job_id;
  l_param :=          case when p1 is null then 'n' else 'x'end;
  l_param := l_param||case when p2 is null then 'n' else 'x'end;
  l_param := l_param||case when p3 is null then 'n' else 'x'end;
  
  l_param := 
  case  when l_param='nnn' then ''
        when l_param='xnn' then ','||trim(p1)
        when l_param='nxn' then ','||'null'  ||','||trim(p2)
        when l_param='nnx' then ','||'null'  ||','||'null'  ||','||trim(p3)
        when l_param='xxn' then ','||trim(p1)||','||trim(p2)
        when l_param='xnx' then ','||trim(p1)||','||'null'  ||','||trim(p3)
        when l_param='nxx' then ','||'null'  ||','||trim(p2)||','||trim(p3)
        when l_param='xxx' then ','||trim(p1)||','||trim(p2)||','||trim(p3)
  end;
  return l_param;
exception
   when others then
     debugmsg(-1,l_part, 'Ошибка получения параметров в таблице j_list job_id='||p_job_id, p_job_id);
     return '';
end;

--------------------------------------------------------------------------------
function GetManagerParamN(p in varchar2) return number
is
  pp varchar2(200) := upper(trim(p));
  l_part  j_log.part%type := 'GetManagerParamN';
begin
  if paramList.exists(pp) then 
--    debugmsg(4,l_part,'GetManagerParamN('||pp||')='||paramList(pp));
    return to_number(paramList(pp)); 
  else
    debugmsg(0,l_part,'параметр '||pp||' не найден',0);
    return null;
    --raise_application_error(-20000,'параметр '||pp||' не найден');
  end if;
end;
--------------------------------------------------------------------------------
function GetManagerParamV(p in varchar2) return varchar2
is
  pp varchar2(200) := upper(trim(p));
  l_part  j_log.part%type := 'GetManagerParamV';
begin
  if paramList.exists(pp) then 
--    debugmsg(4,l_part,'GetManagerParamV('||pp||')='||paramList(pp));
    return upper(trim(paramList(pp)));
  else
    debugmsg(0,l_part,'параметр '||pp||' не найден',0);
    return null;
    --raise_application_error(-20000,'параметр '||pp||' не найден');
  end if;
end;
--------------------------------------------------------------------------------
function GetManagerParamD(p in varchar2) return date
is
  pp varchar2(200) := upper(trim(p));
  l_part  j_log.part%type := 'GetManagerParamD';
begin
  if paramList.exists(pp) then 
--    debugmsg(4,l_part,'GetManagerParamD('||pp||')='||paramList(pp));
    return to_date(paramList(pp));
  else
    debugmsg(0,l_part,'параметр '||pp||' не найден',0);
    return null;
    --raise_application_error(-20000,'параметр '||pp||' не найден');
  end if;
end;
--------------------------------------------------------------------------------
function GetManagerParamB(p in varchar2) return boolean
is
  pp varchar2(200) := upper(trim(p));
  l_part  j_log.part%type := 'GetManagerParamB';
begin
  if paramList.exists(pp) then 
--    debugmsg(4,l_part,'GetManagerParamB('||pp||')='||paramList(pp));
    return case 
          when upper(paramList(pp)) = 'TRUE' then true
          when upper(paramList(pp)) = 'FALSE' then false
        else 
          false --debugmsg(0,'параметр '||p||' имеет значение отличное от true\false')
        end;
  else
    debugmsg(0,l_part,'параметр '||pp||' не найден',0);
    return null;
    --raise_application_error(-20000,'параметр '||pp||' не найден');
  end if;
end;

--------------------------------------------------------------------------------
-- загрузка пареметров (0-всех, 1-только тех, что изменились
procedure LoadManagerParams(p_onlyChanged in number)
is
  PRAGMA AUTONOMOUS_TRANSACTION;
  l_param  j_dispatcher.paramname%type;
  l_value  j_dispatcher.paramvalue%type;
  c_load   TRefCursor;
  l_part  j_log.part%type := 'LoadManagerParams';
begin
--  debugmsg(2,l_part, 'LoadParams (p_onlyChanged='||to_char(p_onlyChanged)||')');
  if p_onlyChanged = 0 then 
    paramList.delete;
    open c_load for select paramname, paramvalue from j_dispatcher order by paramname;
  else
    open c_load for select paramname, paramvalue from j_dispatcher where changed=1 order by paramname;
  end if;
  loop
    fetch c_load into l_param, l_value;
    exit when c_load%notfound;
    paramList(upper(trim(l_param))) := trim(l_value);
--    debugmsg(3,l_part,'значение "'||upper(trim(l_param))||'" установлено в "'||trim(l_value)||'"');
  end loop;
  close c_load;
--  debugmsg(3,l_part,'в списке '||paramList.count||' параметров');
  update j_dispatcher set changed=0;
  commit;
  l_debug      := GetManagerParamB('Debug');
  l_debugLevel := GetManagerParamN('DebugLevel');
end;

--------------------------------------------------------------------------------
-- проверка линка к тестовой базе
function TestLinkAndServer(p_job in out TJob, p_link varchar2) return boolean
is
  l_part  j_log.part%type := 'TestLinkAndServer';
  l_res   boolean         := false;
  l_link  varchar2(50)    := p_link;
  l_cnt   number;
begin
--  debugmsg(1,l_part, 'Тестируем линк и респонс базы по линку '||p_link,p_job.job_id); 
  l_link := GetManagerParamV(l_link);
  select count(1) into l_cnt from user_db_Links 
   where DB_LINK like upper(l_link||'%');
  if l_cnt=0 then 
    debugmsg(0, l_part, 'Нет ДБ линка из параметра "'||l_link||'"',p_job.job_id); 
    debugmsg(0, l_part, 'Необходимо создать ДБ линк для пользователя DEPSTAT и прописать его в параметре "'||l_link||'" таблицы J_DISPATCHER',p_job.job_id); 
  else
    begin
      execute immediate 'select count(1) from dual@'||l_link;
      l_res := true;
--      debugmsg(2, l_part, 'Линк Работает'); 
    exception 
      when others then
      begin
        debugmsg(-1,l_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace(),p_job.job_id);
        debugmsg(-1,l_part, 'Ошибка при проверке линка к базе ('||p_link||')',p_job.job_id);
      end;
    end;
  end if;
  return l_res;
exception
  when others then -- caution handles all exceptions
    debugmsg(-1,l_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace(),p_job.job_id);
    return false;
end;


----------------------------------------------------------------------------------------------------
function GetAnswerAble(p_job_id number) return varchar2
is
  l_ans        j_recipient.RECIPIENT%type := '';
  l_part       j_log.part%type := 'GetAnswerAble';
  g_myEmail    varchar2(40) := gpv_myEmail;
  g_answerAble j_recipient.recipient%type := gpv_myEmail;  
begin
  begin
    select RECIPIENT into l_ans from j_recipient where job_id = p_job_id and answerable=1 and rownum=1;
    if lower(l_ans)like 'ftp:%' then 
      l_ans := gpv_myEmail;
    end if;
  exception
    when NO_DATA_FOUND then
      begin
        select RECIPIENT into l_ans from j_recipient where job_id = 0 and answerable=1 and rownum=1;
      exception
        when others then
          l_ans := g_myEmail;
      end;
    when others then
      l_ans := g_myEmail;
  end;
  return l_ans;
end;  

----------------------------------------------------------------------------------------------------
procedure SendSms( p_msisdn_or_user varchar2 default user, p_text varchar2) 
is
  PRAGMA AUTONOMOUS_TRANSACTION;
  l_msisdn  varchar2(200);
begin
  if p_msisdn_or_user like '792%' then 
    l_msisdn := p_msisdn_or_user;
  elsif trim(lower(p_msisdn_or_user))='depstat'    then l_msisdn := '79282011384';
  elsif trim(lower(p_msisdn_or_user))='sparshukov' then l_msisdn := '79282011384';
  elsif trim(lower(p_msisdn_or_user))='rkrikunov'  then l_msisdn := '79282012396';
--  elsif trim(lower(p_msisdn_or_user))='skhizhnjak' then l_msisdn := '79282012372';
  elsif trim(lower(p_msisdn_or_user))='vivakin'    then l_msisdn := '79282012264';
  else 
    l_msisdn := null;
  end if;
  if l_msisdn is not null then
--    execute immediate 'insert into mk$alarm_list@tobis values (0, '''||l_msisdn||''', substr('''||p_text||''',1,990))';
-- comment with Aleksandr.Taranenko db link password has expired
    commit;
  end if;
end;

----------------------------------------------------------------------------------------------------
function GetMsisdnByRecip(p_recip varchar2) return varchar2
is
  l_part       j_log.part%type := 'GetMsisdnByRecip';
  l_ans        j_recipient_sms.msisdn%type := '';
  L_daypart    number;
begin
  begin
    L_daypart := case when sysdate between trunc(sysdate)+7/24 and trunc(sysdate)+9/24-1/86400   then 1
                      when sysdate between trunc(sysdate)+9/24 and trunc(sysdate)+18/24-1/86400  then 2
                      when sysdate between trunc(sysdate)+18/24 and trunc(sysdate)+22/24-1/86400 then 3
                      else 4 end;
    select msisdn into l_ans 
    from j_recipient_sms 
    where trim(lower(recipient))=trim(lower(p_recip)) 
      and enabled=1 
      and bitand(L_daypart,daypart)>0 
      and rownum=1;
  exception
    when others then
      l_ans := gpv_myMsisdn;
  end;
  return l_ans;
end; 

----------------------------------------------------------------------------------------------------
procedure SendResult(p_job in out TJob, p_report in out TAdvReport)
is
  l_res     number := 0;
  l_part    j_log.part%type := 'SendResult';  
  l_err     varchar2(2000);
  L_daypart number;
  l_EmailMaxAttachSize number:=0;
  l_EmailSumAttachSize number:=0;
  l_FtpMaxAttachSize   number:=0;
  l_FtpSumAttachSize   number:=0;
begin
  execute immediate 'alter session disable parallel query';
  execute immediate 'ALTER SESSION disable PARALLEL DDL';

--  debugmsg(2,l_part, 'отправка отчета...', p_job.job_id); 
  if p_report.rl is not null then 
    l_res := p_report.sendreport;
    if  l_res = 1  then
      debugmsg(1,l_part, 'Отчет успешно отправлен. ', p_job.job_id); 
    else 
      debugmsg(1,l_part, 'Отчет не отправлен. код ошибки:'||l_res, p_job.job_id); 
    end if;
  end if;

  -- смс информирование об отчете
  L_daypart := case when sysdate between trunc(sysdate)+7/24 and trunc(sysdate)+9/24-1/86400   then 1
                    when sysdate between trunc(sysdate)+9/24 and trunc(sysdate)+18/24-1/86400  then 2
                    when sysdate between trunc(sysdate)+18/24 and trunc(sysdate)+22/24-1/86400 then 3
                    else 4 end;
  for i in (select recipient from j_recipient where job_id = p_job.job_id 
                and nvl(enabled,0)=1
                and nvl(MAXSENDDATE,to_date('31/12/2999','dd/mm/yyyy'))>sysdate
                and bitand(L_daypart, SMS_DAYPART) > 0
            ) 
  loop
    debugmsg(1,l_part, 'СМС ('||i.recipient||'): '||GetMsisdnByRecip(i.recipient), p_job.job_id);
    SendSms(GetMsisdnByRecip(i.recipient), nvl(p_job.job_comment,p_job.job_name)||'('||to_char(p_job.job_id)||'):'||p_report.result_txt);
  end loop;

exception
  when others then -- caution handles all exceptions
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace()||' в джобе '||to_char(p_job.job_id);
    debugmsg(-1,l_part, 'Exception:'||l_err, p_job.job_id);
end;

----------------------------------------------------------------------------------------------------
procedure SendResult(p_job in out TJob, p_report in out TReport)
is
  l_part    j_log.part%type := 'SendResult';
  l_only    number := 0;
  l_list    TStringList := TStringList();
  l_res     number := 0;
  l_err     varchar2(2000);
  l_recip   varchar2(200);
  L_daypart number;
  l_answerperson    varchar2(200);
  cursor c_send(p_rep number) is
    select * from j_recipient where nvl(enabled,0)=1 
       and job_id = p_rep 
       and nvl(MAXSENDDATE,to_date('31/12/2999'))>sysdate;
begin
  execute immediate 'alter session disable parallel query';
  execute immediate 'ALTER SESSION disable PARALLEL DDL';
  l_answerperson := GetAnswerAble(p_job.job_id);

  if p_job.job_id in (id_ClearDiedJobs, id_RunOnTestServ)  then return; end if;
  debugmsg(1,l_part, 'Будем отправлять отчет по job_id='||p_job.job_id||' c статусом ('||
       case when p_report.result=-1 then 'error'  
            when p_report.result=0  then 'continue' 
            when p_report.result=1  then 'success' 
            else to_char(p_report.result) 
       end||') через '||
       case when p_report.viasystem=1 then 'e-mail' 
            when p_report.viasystem=2 then 'ftp' 
       else to_char(p_report.viasystem) end, p_job.job_id); 
  begin
    l_only := nvl(p_job.sendonlyanswerable,0);
    select SENDONLYANSWERABLE into l_only from j_list where job_id = p_job.job_id;
  exception 
    when others then l_only := nvl(p_job.sendonlyanswerable,0);
  end;

  if l_only = 1 then -- выставлен флаг отправка только ответственному
      debugmsg(2,l_part, 'Отчет будет отправлен только ответственному', p_job.job_id); 
  end if;

  -- выявление списка получателей отчета
  if p_report.viasystem = 1 then -- отчет с вложенинием типа почта
      for i in c_send(p_job.job_id) loop
        if l_only=0 then 
          l_list.extend;
          l_list(l_list.count) := '<'||i.RECIPIENT||'>';
          debugmsg(3,l_part, 'добавлен адресат:'||i.RECIPIENT, p_job.job_id); 
        elsif (l_only=1 and nvl(i.ANSWERABLE,0)=1) then
          l_list.extend;
          l_list(l_list.count) := '<'||i.RECIPIENT||'>';
          debugmsg(3,l_part, 'добавлен ответственный адресат:'||i.RECIPIENT, p_job.job_id); 
        end if;
      end loop;
      -- если никого не нашли - отправляем мне по ЕМАЙЛ
      if l_list.count = 0 then 
        debugmsg(-1,l_part, 'нет получателей для отчета! Отправляем емайл Ответственному за джоб', p_job.job_id); 
        l_list.extend;
        l_list(l_list.count) := l_answerperson; --GetAnswerAble(p_job.job_id);
      end if;
      -- установка списка получателей   
      debugmsg(3,l_part, 'установка списка получателей ('||to_char(l_list.count)||' адресатов)',p_job.job_id); 
      p_report.setrecipients(l_list);  
    
  elsif p_report.viasystem = 2 then  -- отчет с вложением типа ФТП
    if p_report.result <> res_success then 
        debugmsg(-1,l_part, 'Получен ошибочный отчет для ФТП адресатов. Уведомим по почте '||gpv_myEmail, p_job.job_id); 
        l_list.extend;
        l_list(l_list.count) := gpv_myEmail;
    else  
      for i in c_send(p_job.job_id) loop
        l_recip := substr(i.RECIPIENT,1,instr(i.RECIPIENT,':',7))||'***'||substr(i.RECIPIENT,instr(i.RECIPIENT,'@'));
        if l_only=0 then 
          l_list.extend;
          l_list(l_list.count) := i.RECIPIENT;
          debugmsg(3,l_part, 'добавлен адресат:'||l_recip, p_job.job_id); 
        elsif (l_only=1 and nvl(i.ANSWERABLE,0)=1) then
          l_list.extend;
          l_list(l_list.count) := i.RECIPIENT;
          debugmsg(3,l_part, 'добавлен ответственный адресат:'||l_recip, p_job.job_id); 
        end if;
      end loop;
      -- если никого не нашли - отправляем мне по ФТП
      if l_list.count = 0 then 
        debugmsg(-1,l_part, 'нет получателей для отчета! Отправляем по фтп Паршукову', p_job.job_id); 
        l_list.extend;
        l_list(l_list.count) := gpv_myFTP;
      end if;
    end if;
      -- установка списка получателей   
      debugmsg(3,l_part, 'установка списка получателей ('||to_char(l_list.count)||' адресатов)',p_job.job_id); 
      p_report.setrecipients(l_list); 
  end if;
   

  -- собственно отправка
  if p_report.r_mail is null and p_report.r_ftp is null then 
    debugmsg(2,l_part, 'нет данных в полях r_mail или r_ftp для отправки', p_job.job_id); 
--    return;
  else
    if l_only=1 and p_report.r_mail is not null then 
      debugmsg(2,l_part, 'расширяем тело письма', p_job.job_id); 
      dbms_lob.append(p_report.r_mail.mailbody, to_clob(chr(13)||chr(10)||' ---- отчет назначен на ответственного. Ниже список получателей в плановом режиме:'||chr(13)||chr(10) ));
      dbms_lob.append(p_report.r_mail.mailbody, get_clob('select RECIPIENT from j_recipient where nvl(enabled,0)=1 
         and job_id = '||p_job.job_id||' and nvl(MAXSENDDATE,to_date(''31/12/2999''))>sysdate order by RECIPIENT',false));
      dbms_lob.append(p_report.r_mail.mailbody, to_clob('---- конец списка -----'||chr(13)||chr(10) ));
    end if;

    if p_report.r_mail is not null then 
--      dbms_lob.append(p_report.r_mail.mailbody, to_clob(chr(13)||chr(10)||'------ job_id = '||to_char(p_job.job_id)||'------'||chr(13)||chr(10) ));
      j_manager.ctx_set('footer1','Ваши вопросы по отчету (job_id = '||to_char(p_job.job_id)||') Вы можете направлять по адреcу '||l_answerperson);
      j_manager.ctx_set('footer2','Коллеги! в связи с закрытие локальных хранилищ данных с 31/07/2016 рассылка данного отчета будет прекращена');
      j_manager.ctx_set('footer3','Прошу использовать КХД (http://sapbo:8080/BOE/BI). Доступ - через МегаХелп.');
      j_manager.ctx_set('footer4','портал: https://branches.meganet.megafon.ru/reporting/default.aspx');
      j_manager.ctx_set('footer5','Описания юниверсов:  https://branches.meganet.megafon.ru/reporting/Lists/universes/AllItems.aspx');
      j_manager.ctx_set('footer6','Описания отчетов:  https://megawiki.megafon.ru/x/BAM-Ag');
    end if;

    debugmsg(2,l_part, 'отправка отчета...', p_job.job_id); 
    l_res := p_report.sendreport(p_report.reportsend);

    if  l_res = 0  then
      debugmsg(1,l_part, 'Отчет успешно отправлен. ', p_job.job_id); 
      update j_recipient 
        set lastsend = sysdate 
      where job_id = p_job.job_id
        and (upper('<'||RECIPIENT||'>') in (select upper(COLUMN_VALUE) from table(cast(l_list as TStringList)))
             or
             upper(RECIPIENT) in (select upper(COLUMN_VALUE) from table(cast(l_list as TStringList))));
    else 
      debugmsg(1,l_part, 'Отчет не отправлен. код ошибки:'||l_res, p_job.job_id); 
      p_report.setrecipients(TStringList('<'||l_answerperson/*GetAnswerAble(p_job.job_id)*/||'>'));
      l_res := p_report.sendreport(p_report.reportsend);
      if l_res =0 then 
        debugmsg(1,l_part, 'Отчет успешно отправлен ответственному. ', p_job.job_id); 
      end if;
    end if;
  end if;

  -- смс информирование об отчете
      L_daypart := case when sysdate between trunc(sysdate)+7/24 and trunc(sysdate)+9/24-1/86400   then 1
                        when sysdate between trunc(sysdate)+9/24 and trunc(sysdate)+18/24-1/86400  then 2
                        when sysdate between trunc(sysdate)+18/24 and trunc(sysdate)+22/24-1/86400 then 3
                        else 4 end;
--     debugmsg(1,l_part, 'Анализ смс информирования для части суток : '||l_daypart, p_job.job_id); 
      for i in (select recipient from j_recipient where job_id = p_job.job_id 
                    and nvl(enabled,0)=1
                    and nvl(MAXSENDDATE,to_date('31/12/2999'))>sysdate
                    and bitand(L_daypart, SMS_DAYPART) > 0
                ) 
      loop
        debugmsg(1,l_part, 'СМС ('||i.recipient||'): '||GetMsisdnByRecip(i.recipient), p_job.job_id);
        SendSms(GetMsisdnByRecip(i.recipient), nvl(p_job.job_comment,p_job.job_name)||'('||to_char(p_job.job_id)||'):'||p_report.result_txt);
      end loop;

  commit;
 
exception
  when others then -- caution handles all exceptions
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace()||' в джобе '||to_char(p_job.job_id);
    debugmsg(-1,l_part, 'Exception:'||l_err, p_job.job_id);
end;

--------------------------------------------------------------------------------
procedure inform(p_text in  varchar2, p_method in number default 3)
is
begin
  if p_text is null then return; end if;

  if bitand(p_method,1)=1 then 
    SendSms('sparshukov',p_text);
  end if;

  if bitand(p_method,2)=1 then 
    emailsender.sendto(gpv_myEmail, 'Предупреждение: диспетчер на '||sys_context('UserEnv','server_host'), p_text);
  end if;
  
  debugmsg(1,'inform', p_text, -1); 
end;

--------------------------------------------------------------------------------
function HasFreshMKS(p_needDate date default sysdate, p_TestServNum number default 1) return number
is
  l_part          j_log.part%type := 'HasFreshMKS';
  l_param    varchar2(100);
  l_paramVal varchar2(100);
  l_need_dat date := trunc(p_needDate);
  l_mks_dat  date;
  j          TJob;
  l_res      number := 0;
  l_cnt      number;
  l_TestServNum varchar2(5);
  l_err     varchar2(1000);
begin
  if p_TestServNum=0 then 
    return 0;
  end if;
  l_TestServNum := case when nvl(p_TestServNum,1) = 1 then '' else to_char(p_TestServNum) end;
  select count(1) into l_cnt
    from j_dispatcher where lower(PARAMNAME) = 'dblink_to_test1' and PARAMVALUE is not null and rownum<=1;
  if l_cnt>0 then 
    select paramname, paramvalue into l_param, l_paramVal from j_dispatcher where lower(PARAMNAME) = 'dblink_to_test'||l_TestServNum and PARAMVALUE is not null and rownum<=1;
    if TestLinkAndServer(j, l_param) then 
      execute immediate 'select dat from testserver@'||l_paramVal into l_mks_dat;
      if trunc(l_mks_dat)=l_need_dat then 
        l_res := 1;
      end if;
    end if;
  end if;
  return l_res;
exception
  when others then 
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace()||' в джобе ';
    debugmsg(-1,l_part, 'Exception: '||l_err||' при проверке HasFreshMKS на сервере с номером '||l_TestServNum, 0);
    return 0;  
end;


--------------------------------------------------------------------------------
function getStatServNum return number
is
  res number;
  l_part          j_log.part%type := 'getStatServNum';
begin
  select count(1) into res from user_jobs where lower(what) like '%dispatch_report%';
  if res=0 then 
    return -1;
  end if;
  select count(1) into res from user_jobs where lower(what) like '%dispatch_report%main%';
  if res=1 then 
    return 0;
  end if;
  select count(1) into res from user_jobs where lower(what) like '%dispatch_report%secondary%';
  if res=1 then
    select to_number(regexp_substr(substr(what, instr(what,'secondary')+10), '[0-9]+')) into res 
    from user_jobs where what like '%dispatch_report%secondary%';
    return res;
  end if;
exception
  when others then 
    debugmsg(1,l_part, 'Ошибка получения номера стат.сервера из DBMS_JOB', sys_context('jm_ctx', 'job_id')); 
    return -1;
end;

--------------------------------------------------------------------------------
procedure dispatch_reports(nd in out date, p_server_kind in varchar2 default 'main', p_server_num in number default 0)
is
  l_part          j_log.part%type := 'dispatch_reports';
  l_job           number          := 0;   --для хранения ID joba, который вернется через DBMS_JOB.SUBMIT
  l_startTime     date;          --дата для запуска джоба под конкетную задачу
  l_sql           varchar2(4000); --тело джоба
  l_err           varchar2(1000); 
  l_res           number        := 0;
  l_cnt           number        := 0;
  lg_maxParallel  number        := 0;
  l_job_parallel  number        := 0;
  l_jlist_job_id  number        := 0;
  l_host          varchar2(50)  :='';
  l_convertResult varchAR2(1000):= '';
  L_r             varchar2(100) := '';
  l_master_Nextd  date;
  l_master_cronss number;

  -- задачи, которые можно запустить
  c_runTask      TRefCursor;
  i              j_list%rowtype;
  l_callSendRes  varchar2(200)   := '';
  l_job_num_str  varchar2(200)   := '';
  l_runMethod    varchar2(200)   := '';
  
  l_schema       varchar2(100);
  l_objct        varchar2(100);
  l_procdr       varchar2(100);
  l_objtype      varchar2(100);
  l_rettype      varchar2(100);
  l_hasFreshMKS  number;
  l_DBLINK_TO_TEST varchar2(100);

  cursor c_died is
    select job_id,job_name,run_by_job 
    from j_list m 
    where 
      run_by_job <> 0 and  run_by_job <> -1
      and (not exists (select 1 from user_jobs where job =m.run_by_job))
      and (not exists (select 1 from user_scheduler_jobs where  comments = to_char(m.run_by_job)))
      and (not exists (select 1 from user_scheduler_jobs where  job_name like 'jm_%'||to_char(m.run_by_job)||'%'))
    for update of run_by_job;
  
begin
  dbms_application_info.set_client_info('dispatch_reports');  
  l_host := sys_context('userenv','SERVER_HOST')||', база='||sys_context('userenv','DB_UNIQUE_NAME');

  -- удаляю уже мертвые джобы
  for i in c_died loop
    debugmsg(0,l_part, 'обновляю - доступной для записи job='||i.job_id||' ('|| i.job_name ||')', i.job_id);
    UPDATE j_list SET run_by_job = 0 WHERE CURRENT OF c_died;
  end loop;
  commit;

  -- чистим историю диспетчера за период более 2х месяцев назад
  delete from j_log where dinsdate<sysdate-60;
  
  -- проверка на приостанов J_DISPATCHER.SUSPEND = TRUE
  if GetManagerParamB('suspend') then 
    begin select cmsg into l_tmp from j_log where job_id = -1 and dinsdate > sysdate-4/24 and cmsg like '%диспетчер%в%режиме%suspend%true%';
    exception when others then null; end;
    if l_tmp is null then 
       inform('диспетчер на '||l_host||' в режиме suspend=true');
    end if;
    nd := sysdate+5/60/24; -- каждые 5 минут (nd - параметр NEXT_DATE из user_jobs. Необходимо передвинуть)
    return; 
  end if;
  
  -- проверка на допустимость имени сервера на котором запустили диспетчера
  select count(1) into l_cnt from j_dispatcher where 1=1
    and (lower(PARAMNAME) = 'mainserver' or lower(PARAMNAME) like 'backupserver%')
    and instr(upper(PARAMVALUE), upper(sys_context('UserEnv','server_host')) )>0;
  if l_cnt=0 then 
    begin select cmsg  into l_tmp from j_log where job_id = -1 and dinsdate > trunc(sysdate) and cmsg like '%хост%не%является%разрешенным%';
    exception when others then null; end;
    if l_tmp is null then 
       inform('хост ('||l_host||') не является разрешенным для работы в режиме '||p_server_kind||' '||to_char(p_server_num));
    end if;
    return;
  end if;
  
  -- если IP сервера не входит в особый список, с него могут не уходить письма
  -- в таком случае устанавливаем контекстную переменную.
  -- метод отправки результата по почте скинет письмо с вложениями в таблицу NOT_SENDED_EMAIL (требуется доработка)
  begin
    l_err := utl_inaddr.get_host_address;
    select nvl(max(instr(PARAMVALUE, l_err)),0) into l_cnt from j_dispatcher j where lower(PARAMNAME) = 'email_ip';
    if l_cnt=0 then 
        begin select cmsg  into l_tmp from j_log where job_id = -1 and dinsdate > trunc(sysdate) and cmsg like '%хоста%могут%не%уходить%письма%';
        exception when others then null; end;
        if l_tmp is null then 
           inform('С хоста ('||l_host||')('||l_err||') могут не уходить письма');
           dbms_session.SET_CONTEXT('jm_ctx', 'save_email', 'true');
        end if;
    end if;
  exception 
    when others then 
      l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
      debugmsg(-1,l_part, 'Exception: не удалось проверить сервер на доступ к отправке почты. '||l_err,0);
      inform('С хоста ('||l_host||')('||l_err||') могут не уходить письма');
      dbms_session.SET_CONTEXT('jm_ctx', 'save_email', 'true');
  end;

  -- проверка установки на использование параллелизма ALLOWPARALLEL=true
  -- и получение максимума-ограничения, безусловно переопределяющего пожелание джоба MAXPARALLEL или MAXPARALLELBACKUPSRV
  -- максимум зависит от режима работы (боевой\статистический)
  if GetManagerParamB('allowParallel') then 
    if upper(p_server_kind) = 'MAIN' then 
      lg_maxParallel := GetManagerParamN('MaxParallel');
    elsif upper(p_server_kind) = 'SECONDARY' then 
      lg_maxParallel := GetManagerParamN('MaxParallelBackupSrv');
    end if;
  end if;

  -- в режиме основного сервера открываем курсор на задачи где ONSTATSERV = 0
  if upper(p_server_kind) = 'MAIN' then 
    open c_runTask for select * from j_list where nextdat <= sysdate and nvl(enabled,0)=1 and nvl(run_by_job,0) = 0   
    and (ONSTATSERV = 0) -- or job_id = id_ClearDiedJobs) ClearDiedJobs внес в тело джоба dispatch_reports
    and MAXNEXTDAT > nextdat;
  -- в режиме статистического сервера открываем курсор на задачи где ONSTATSERV = номер статистического сервера
  -- номер передается при создании джоба dispatch_reports c основного сервера, третьим параметром (см.тело джоба dispatch_reports)
  elsif upper(p_server_kind) = 'SECONDARY' then 
    if isstatserv=0 then
       l_startTime := GetStartDate;
       execute immediate 'create table TESTSERVER as select to_date('''||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss')||''',''dd.mm.yyyy hh24:mi:ss'') dat from dual';
       debugmsg(-1, l_part, GetMsisdnByRecip(getanswerable(0))||'-Поднялся диспетчер на сервере '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'),0);
       execute immediate 'grant select on testserver to public';
       -- извещаем всех заинтересованных лиц, что поднялся диспетчер
       /*for k in (select * from j_recipient_sms where wake_up=1 and enabled=1)
       loop
         SendSms(k.msisdn, 'Поднялся диспетчер на сервере '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       end loop;*/
       --SendSms(GetMsisdnByRecip(getanswerable(0)), 'Поднялся диспетчер на сервере '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- паршуков
       SendSms('79282011384', 'Поднялся диспетчер на '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- ивакин
       SendSms('79381112264', 'Поднялся диспетчер на '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- крикунов
       SendSms('79289072396', 'Поднялся диспетчер на '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- тарасенко
       --SendSms('79282012680', 'Поднялся диспетчер на '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- левенец
--       SendSms('79282015240', 'Поднялся диспетчер на '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- тараненко
       SendSms(gpv_MsisdnTaranenko, 'Поднялся диспетчер на '||l_host||' в режиме SECONDARY='||p_server_num||' с актуальностью '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'));
       -- меняем дату следующего запуска для процессов, которые успели ночью отработать на старом МКСТАТ
       -- а потом его обновили       
       for i in (select j.job_id, h.JOBRES, h.JOBRESTXT, h.LASTMAXSEND, j.LASTSTART, j.JOB_NAME, j.NEXTDAT
                 from j_list j, j_list_history h, (select job_id, max(iid) max_iid from j_list_history group by job_id) hm
                 where 1=1
                   and j.LASTSTART between trunc(sysdate) and sysdate --(select min(dat) from testserver) -- запуск на старом МКСТАТ
                   and j.NEXTDAT > trunc(sysdate)+1 -- следующий старт завтра
                   and j.enabled=1                  -- задача разрешенная
                   and j.maxnextDat> trunc(sysdate) -- задача разрешенная
                   and j.ONSTATSERV<>0              -- запускается на стат сервере
                   and h.JOBRES=0                   -- прошлый запуск окончился res_continue
                   and j.job_id = h.job_id 
                   and h.iid = hm.max_iid
                 )
       loop
         update j_list set NEXTDAT=sysdate+5/60/24 where job_id = i.job_id;
         debugmsg(-1, l_part, 'Делаю доступной для запуска, т.к. она отработала с кодом res_continue '||to_char(i.LASTSTART,'dd.mm.yyyy hh24:mi:ss')||' и дату следующео старта установила в след.сутки',i.job_id);
       end loop;
       commit;
    end if;
    open c_runTask for select * from j_list where nextdat <= sysdate and nvl(enabled,0)=1 and nvl(run_by_job,0) = 0   
    and (ONSTATSERV = p_server_num) -- or job_id = id_ClearDiedJobs) ClearDiedJobs внес в тело джоба dispatch_reports
    and MAXNEXTDAT > nextdat;
    l_callSendRes  := 'J_MANAGER.RunJobSaveResult(j, r.result, r.result_txt);';
  else
    debugmsg(-1, l_part, 'Ошибочный тип сервера p_server_kind='||p_server_kind,0);
    open c_runTask for select * from j_list where 1=0;
  end if;
    
  l_hasFreshMKS  := HasFreshMKS(sysdate, p_server_num);
  l_jlist_job_id := 0; -- для exception там будет недоступна i.job_id
  --=================================================================================================
  -- основной цикл : пробежимся по задачам, которые необходимо запустить поставим каждую через минуту друг от друга
  loop
   fetch c_runTask into i;
   exit when c_runTask%notfound;

   l_jlist_job_id := i.job_id; --
   if i.job_id not in (id_ClearDiedJobs, id_RunOnTestServ) then 
     debugmsg(0,l_part, '',i.job_id);
     debugmsg(0,l_part, 'задача №'||i.job_id,i.job_id); 
   end if;
   l_cnt := l_cnt + 1;
   ----------------------------------------------------------------
   begin
      l_job:=0;
      -- при запуске с мастера, ведомая копия расчитывает дату следующего старта и апдейтит ее на мастер и копию.
      -- может получиться так, что время запуска у копии произойдет чуточку ранее. копия проверит, что нет всех данных (например, наступил следующий день)
      -- передвинет дату следующего старта. Мастер так и не подберется к тому моменту, чтобы запуститься, т.к. будет опазывать на несколько секунд из-за того, что копия будет запускаться ранее и сдвигать время
      execute immediate 'select NEXTDAT, CANRUNONSTAT from j_list@'||GetManagerParamV('dblink_to_main')||' where job_id=:j' into l_master_nextD, l_master_cronss using i.job_id;
      if upper(p_server_kind) <> 'MAIN' and l_master_cronss=1 and l_hasFreshMKS=1 and abs(l_master_nextD-i.nextdat) < 0.5/24 then 
        debugmsg(0, l_part, 'Обнаружено: сервер=тестовый, CANRUNONSTAT у мастера = 1, l_hasFreshMKS(sysdate, '||to_char(p_server_num)||')=1, разница между NEXTDAT не более получаса(тестовый='||to_char(i.nextdat,'dd.mm.yyyy hh24:mi:ss')||', мастер='||to_char(l_master_nextD,'dd.mm.yyyy hh24:mi:ss')||'). 
Вероятно статистический "бежит впереди паровоза" после прошлого запуска с мастера. Отключаю ему запуск на стат.сервере',i.job_id);
        update j_list set NEXTDAT=null where job_id = i.job_id;
        commit;
        continue;
      end if;
      
      -- анализируем имя джоба
      l_res := ParseAndTestJobName(i.job_name, l_schema, l_objct, l_procdr, l_objtype, l_rettype);
      if l_res =0 then 
        debugmsg(0, l_part, 'У задачи задано имя('||i.job_name||'), которого либо нет, либо нет прав у '||user||' на выполнение',i.job_id);
        emailsender.sendto(GetAnswerAble(i.job_id),
           g_subj||'Ошибка:'||i.job_name, 
           'У задачи задано имя('||i.job_name||'), которого либо нет, либо нет прав у '||user||' на выполнение');
        continue;
      end if;
      -- через минуту
      l_startTime := sysdate+((l_cnt*60)/86400);

      -- определяем уровень параллельности
      if lg_maxParallel > 0 then 
        IF nvl(i.maxParallel,0)>0 and nvl(i.maxParallel,0)<=lg_maxParallel then
          l_job_parallel := i.maxParallel;
        elsif( nvl(i.maxParallel,0)>0 and nvl(i.maxParallel,0)>lg_maxParallel ) then
          l_job_parallel := lg_maxParallel;
        end if;
      end if;

      -- определяем метод запуска
      l_runMethod := case when i.JOB_COMMENT like 'SHD%' then 'DBMS_SCHEDULER'
                          when i.JOB_COMMENT like 'JOB%' then 'DBMS_JOB'
                          else GetManagerParamV('default_dispatcher') end;
      if l_runMethod='DBMS_SCHEDULER' then 
        l_job := to_number(DBMS_SCHEDULER.GENERATE_JOB_NAME (prefix=>''));
        l_job_num_str  := 'j.run_by_job := '||to_char(l_job)||';';
      else
        l_job_num_str  := 'j.run_by_job := JOB;';
      end if;

      if l_rettype in ('CLOB','BLOB') then 
        l_convertResult := chr(13)||chr(10)||'ra := TAdvReport.rCreate(1, ''success'',             '||i.job_id||', ''Статистика : '||i.job_name||''', ''Полученный результат во вложении'', TRItemList(TRItem.iCreate(r)));'||chr(13)||chr(10);
        l_r := 'ra';
      elsif l_rettype in ('NUMBER','VARCHAR2','DATE') then 
        l_convertResult := chr(13)||chr(10)||'ra := TAdvReport.rCreate(1, ''success=''||to_char(r),'||i.job_id||', ''Статистика : '||i.job_name||''', ''Полученный результат : ''||'||
                           case when l_rettype = 'VARCHAR2' then 'r' 
                                when l_rettype = 'DATE'     then 'to_char(r,''dd.mm.yyyy hh24:mi:ss'')'
                           else 'to_char(r)' end||');'||chr(13)||chr(10);
        l_r := 'ra';
        l_callSendRes  := 'J_MANAGER.RunJobSaveResult(j, ra.result, ra.result_txt);';
      else 
        l_convertResult := chr(13)||chr(10);
        l_r := 'r';
      end if;
      -- формируем SQL для запуска
      l_sql :='declare
 r '||l_rettype||';
 ra TADVReport;
 j TJob   := TJob('||i.job_id||','''||i.job_name||''',
                  '||case when i.nextdat is null then 'null' else 'to_date('''||to_char(i.nextdat,'dd.mm.yyyy hh24:mi:ss') ||''',''dd.mm.yyyy hh24:mi:ss'')'end||' /*nextdat*/,
                  '||case when i.NEXTFORMULA is null then 'null' else ''''||replace(i.NEXTFORMULA,'''','''''')||''''end||' /*NEXTFORMULA*/,
                  '||case when i.Maxnextdat is null then 'null' else 'to_date('''||to_char(i.MaxNextdat,'dd.mm.yyyy hh24:mi:ss') ||''',''dd.mm.yyyy hh24:mi:ss'')'end||' /*MaxNextdat*/,
                  '||case when i.param1 is null then 'null' else ''''||replace(i.param1,'''','''''')||''''end||' /* param1 */, 
                  '||case when i.param2 is null then 'null' else ''''||replace(i.param2,'''','''''')||''''end||' /* param2 */, 
                  '||case when i.param3 is null then 'null' else ''''||replace(i.param3,'''','''''')||''''end||' /* param3 */, 
                  '||case when i.sendOnlyAnswerable is null then '0' else ''||i.sendOnlyAnswerable||''end||'    /*sendOnlyAnswerable*/, 
                  '||case when i.laststart is null then 'null' else 'to_date('''||to_char(i.laststart,'dd.mm.yyyy hh24:mi:ss') ||''',''dd.mm.yyyy hh24:mi:ss'')'end||' /*laststart*/, 
                  null /*run_by_job*/, '||to_char(l_job_parallel)||' /* maxparallel*/,
                  '||case when i.job_comment is null then '0' else ''''||replace(i.job_comment,'''','"')||''''end||');
begin 
 update j_list set LastStart=sysdate where job_id = j.job_id; commit;'
 ||l_job_num_str||'
 J_MANAGER.SetJobBefore(j);
 r := '||i.job_name||'(j '||GetJobParams(i.job_id)||');'
 ||l_convertResult||'
 J_MANAGER.sendResult(j, '||l_r||');
 J_MANAGER.setJobAfter(j, '||l_r||'.result, '||l_r||'.result_txt);'
 ||l_callSendRes||'
 commit;
exception
  when others then 
  log_ovart(-1,j.job_name, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
  raise;
end;';


      if hasFreshMKS(sysdate) = 1 and i.CANRUNONSTAT=1 and upper(p_server_kind) = 'MAIN' then 
        l_DBLINK_TO_TEST := j_manager.GetManagerParamV('DBLINK_TO_TEST');
        debugmsg(1,l_part, 'Обнаружил возможность запуска на основном расчетном сервере "DBLINK_TO_TEST"="'||l_DBLINK_TO_TEST||'". Пытаюсь пнуть на нем',i.job_id); 
        l_res := kick_job(i.job_id, 'dblink_to_test');
        if l_res = res_success then 
          execute immediate 'update j_list@'||l_DBLINK_TO_TEST||'
                                set onstatserv=1, 
                                    SENDONLYANSWERABLE = (select nvl(SENDONLYANSWERABLE,0) from j_list'||l_dblMain||' where job_id = :p_job_id),
                                    MAXPARALLEL=case when MAXPARALLEL=0 then 3 when MAXPARALLEL<=3 then MAXPARALLEL*3 when MAXPARALLEL<=7 then MAXPARALLEL*2 when MAXPARALLEL<=15 then MAXPARALLEL+5 else MAXPARALLEL end 
                             where job_id=:j and nvl(RUN_BY_JOB,0)=0' using i.job_id, i.job_id;
          commit;
--          if sql%rowcount=1 then 
            update j_list 
              set nextdat = null, run_by_job=-1, LastStart=sysdate, lastJobBody=to_clob(l_sql)
            where job_id=i.job_id;
          commit;
--          else
--            debugmsg(1,l_part, 'Пинок мимо. KICK разрешил, но либо нет джоба, либо он работает'||sql%rowcount,i.job_id); 
--          end if;
        else
          debugmsg(1,l_part, 'Пинок не попал в цель. KICK ответил "'||case 
            when l_res=-1 then 'не обнаружен джоб с таким номером'
            when l_res=-2 then 'джоб уже работает'
            when l_res=-3 then 'Ошибка при проверке линка к базе' else to_char(l_res) end||'"', i.job_id); 
        end if;
      else
        -- ставим на выполнение
        if l_runMethod='DBMS_SCHEDULER' then 
            l_err := substr('jm_'||to_char(l_job)||'_'||case when instr(i.job_name,'.',-1)=0 then i.job_name else substr(i.job_name,instr(i.job_name,'.',-1)+1) end,1,29);
            debugmsg(1,l_part, l_err,i.job_id);
            DBMS_SCHEDULER.CREATE_JOB (
                 job_name           => l_err,
                 job_type           => 'PLSQL_BLOCK',
                 --start_date         => systimestamp + interval '1' MINUTE * l_cnt,
                 start_date         => systimestamp,
                 job_action         => l_sql,
                 repeat_interval    => null,
                 enabled            => TRUE,
                 comments           => to_char(l_job)
              );
        else
          dbms_job.submit(l_job, l_sql, l_startTime);
        end if;
        -- протоколируем предварительно в J_LIST (дата старта будет еще раз переписана при фактическом старте)
        update j_list set (run_by_job,LastStart,lastJobBody)  = (select l_job,l_startTime,to_clob(l_sql) from dual) where job_id = i.job_id;
        debugmsg(1,l_part, 'процедурa '||i.job_name||' поставлена('||case when l_job_num_str = 'j.run_by_job := JOB;'then 'job' else 'sch' end||') :JOB_ID='||l_JOB||' на '||to_char(l_startTime,'dd.mm.yyyy hh24:mi:ss'),i.job_id); 
      end if; 
    ----------------------------------------------------------------
    exception
      when others then -- caution handles all exceptions
        l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
        debugmsg(-1,l_part, 'Exception при постановке джоба на '||i.job_name||' '||l_err, l_jlist_job_id);
        debugmsg(-1,l_part, l_sql, l_jlist_job_id); 
    end;
    ----------------------------------------------------------------
  end loop;
  close c_runTask;
  
  nd := sysdate+5/60/24; -- каждые 5 минут (nd - параметр NEXT_DATE из user_jobs. Необходимо передвинуть)
  commit; -- иначе джоб не будет выполняться
exception
  when others then -- caution handles all exceptions
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
      debugmsg(-1,l_part, 'Exception:'||l_err,0);
    nd := sysdate + 1/24/4; -- через 15 мнут
end;


--------------------------------------------------------------------------------
function GetAuthor(p_job_name in varchar2)return varchar2
is
  l_part     varchar2(100) := 'GetAuthor';
  l_author   varchar2(100);
  l_schema   varchar2(100);
  l_objct    varchar2(100);
  l_procdr   varchar2(100);
  l_objtype  varchar2(100);
  l_rettype  varchar2(100);
  L_tmpchar  varchar2(200);
  l_res      number;
  l_job_id   number;
begin
--dbms_output.put_line('job_name=('||p_job_name||')');
  begin
    select min(job_id) into l_res from j_list where upper(trim(job_name))=upper(trim(p_job_name));
    --dbms_output.put_line('l_resq= '||l_res);
  exception
    when others then l_res:=null;
  end;
  l_job_id := coalesce(sys_context('jm_ctx', 'job_id'), l_res, 0);
  --dbms_output.put_line('ctx'||sys_context('jm_ctx', 'job_id'));
  --dbms_output.put_line('l_res= '||l_res);
  --dbms_output.put_line('result='||l_job_id);
  -- по ответственному за джоб
  if l_job_id <> 0 then 
    begin
      select recipient into L_tmpchar from (select recipient from j_recipient 
                               where job_id = l_job_id and answerable=1 
                               order by enabled nulls last, LASTSEND nulls last)
      where rownum<=1;
      L_tmpchar := case 
         when UPPER(L_tmpchar) like '%PARSHUKOV%' then  'SPARSHUKOV'
         when UPPER(L_tmpchar) like '%KRIKUNOV%'  then  'RKRIKUNOV'
         when UPPER(L_tmpchar) like '%TARASENKO%' then  'TARASENKO_SS'
         when UPPER(L_tmpchar) like '%LEVENETS%'  then  'LEVENETS_EE'
         when UPPER(L_tmpchar) like '%IVAKIN%'    then  'VIVAKIN'
       end;
      if length(L_tmpchar)>=0 then 
        return L_tmpchar;
      end if;
    exception 
      when no_data_found then
         null;
    end;
  end if;
--  dbms_output.put_line('job_id='||l_job_id);

  l_res := ParseAndTestJobName(p_job_name, l_schema, l_objct, l_procdr, l_objtype, l_rettype);
  if l_objtype is null then 
    l_res := 0;
    for i in (select * from all_procedures where owner=l_schema 
                   and ((OBJECT_NAME=l_objct or  PROCEDURE_NAME=l_objct) or (OBJECT_NAME=l_objct and PROCEDURE_NAME=l_procdr)))
    loop
      l_objct  := i.OBJECT_NAME;
      l_procdr := i.PROCEDURE_NAME;
      l_objtype:= i.OBJECT_TYPE;
      l_res := l_res + 1;
    end loop;          
/*    if l_res=0 then dbms_output.put_line('нет подходящих процедур к "'||job_name||'"');
    elsif l_res=1 then dbms_output.put_line('найден - '||l_schema||'.'||l_objct||'.'||l_procdr||' в '||l_objtype);
    elsif l_res>1 then dbms_output.put_line('найдено несколько процедур - будет использован тип последней');
    end if; */
  end if;

-- по ключевой строке в теле пакета\процедуры
  begin
    SELECT 
      case 
         when UPPER(text) like '%SPARSHUKOV%' then 'SPARSHUKOV'
         when UPPER(text) like '%SKHIZHNJAK%' then 'SKHIZHNJAK'
         when UPPER(text) like '%RKRIKUNOV%'  then 'RKRIKUNOV'
         when UPPER(text) like '%TARASENKO%'  then 'TARASENKO_SS'
         when UPPER(text) like '%LEVENETS%'   then 'LEVENETS_EE'
         when UPPER(text) like '%VASIN%'      then 'SPARSHUKOV'
         when UPPER(text) like '%VIVAKIN%'    then 'VIVAKIN'
       end  into l_author
     FROM all_source --@todwh_tr_mk t
    WHERE 1=1 
      and UPPER(text) LIKE '%AUTHOR%=%'
      AND TYPE = l_objtype
      and name = nvl(l_objct,l_procdr);
  exception
    when others then
      debugmsg(0, l_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace(), l_job_id);
  end;

-- по имени пакета
  if l_author is null then 
      debugmsg(0, l_part, 'В спецификации пакета не обнаружена строка "%AUTHOR%=%" ', l_job_id);
      l_author := case 
         when l_schema = 'LDWH' and l_objct in ('CRM_UPDATE','SEGMENTATION') then 'SPARSHUKOV'
         when upper(substr(p_job_name,1,instr(p_job_name,'.',-1)-1)) like '%PSV%' then 'SPARSHUKOV'
         when upper(substr(p_job_name,1,instr(p_job_name,'.',-1)-1)) like '%SKH%' then 'SKHIZHNJAK'
         when upper(substr(p_job_name,1,instr(p_job_name,'.',-1)-1)) like '%KRL%' then 'RKRIKUNOV'
         when upper(substr(p_job_name,1,instr(p_job_name,'.',-1)-1)) like '%IVA%' then 'VIVAKIN'
         when UPPER(substr(p_job_name,1,instr(p_job_name,'.',-1)-1)) like '%TSS%' then 'TARASENKO_SS'
       end;  
  end if;
  
  if l_author is null then 
    begin
        SELECT 
          case 
             when UPPER(text) like '%SPARSHUKOV%' then 'SPARSHUKOV'
             when UPPER(text) like '%SKHIZHNJAK%' then 'SKHIZHNJAK'
             when UPPER(text) like '%RKRIKUNOV%'  then 'RKRIKUNOV'
             when UPPER(text) like '%DRASTVOROV%' then 'SPARSHUKOV'
             when UPPER(text) like '%VASIN%'      then 'SPARSHUKOV'
             when UPPER(text) like '%VIVAKIN%'    then 'VIVAKIN'
             when UPPER(text) like '%TARASENKO%'  then 'TARASENKO_SS'
             when UPPER(text) like '%LEVENETS%'   then 'LEVENETS_EE'
           end  into l_author
         FROM all_source --@todwh_tr_mk t
        WHERE 1=1 
          and UPPER(text) LIKE '%AUTHOR%=%'
          AND TYPE = 'PACKAGE'
          and name = l_objct;    
    exception
      when others then null;
    end;
  end if;

  return l_author;
end;


--------------------------------------------------------------------------------
procedure alter_session(p_job_id number, p_param varchar2, p_value varchar2, p_logging number default 0)
is
  l_part   j_log.part%type := 'setJobBefore';
  l_err    varchar2(2000);
  L_sql    varchar2(1000);
begin
  L_sql := 'alter session '||p_param||' '||p_value||'';
  execute immediate L_sql;
  j_manager.addp_set(p_job_id, 'default', 'last '||p_param, p_value);      
  if p_logging=1 then 
    debugmsg(-1, l_part, L_sql, p_job_id );
  end if;
exception 
  when others then 
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
    debugmsg(-1,l_part, L_sql||': Exception:'||l_err, p_job_id);
    j_manager.addp_set(p_job_id, 'default', 'last '||p_param, l_err);      
end;

--------------------------------------------------------------------------------
procedure SetJobBefore(p_job in out TJob)
is
  l_part   j_log.part%type := 'setJobBefore';
  l_res    number;
  l_err    varchar2(2000);
  l_author varchar2(100);
  l_schema varchar2(100);
  l_objct  varchar2(100);
  l_procdr varchar2(100);
  l_obj_type varchar2(100);
  l_rettype varchar2(100);
begin
  update j_list set LastStart=sysdate where job_id = p_job.job_id; 
  p_job.laststart := sysdate;
  commit;
  dbms_session.SET_CONTEXT('jm_ctx', 'job_id',    to_char(p_job.job_id));
  dbms_session.SET_CONTEXT('jm_ctx', 'job_start', to_char(sysdate,'dd.mm.yyyy hh24:mi:ss'));
  debugmsg(0,l_part, '', p_job.job_id);
  debugmsg(0,l_part, 'jobid='||p_job.job_id||'('||p_job.job_name||')'' -- начата работа', p_job.job_id);

  dbms_application_info.set_client_info(p_job.job_name);  
  ----------------------------------------------------------------------------
  l_res := ParseAndTestJobName(p_job.job_name, l_schema, l_objct, l_procdr, l_obj_type, l_rettype);
  dbms_session.SET_CONTEXT('jm_ctx', 'schema', l_schema);
  dbms_session.SET_CONTEXT('jm_ctx', 'object', l_objct);
  dbms_session.SET_CONTEXT('jm_ctx', 'procedure', l_procdr);

  dbms_session.SET_CONTEXT('jm_ctx', 'param1', p_job.param1);
  dbms_session.SET_CONTEXT('jm_ctx', 'param2', p_job.param2);
  dbms_session.SET_CONTEXT('jm_ctx', 'param3', p_job.param3);
  
  ----------------------------------------------------------------------------
  if l_procdr is not null or l_objct is not null then 
     dbms_session.SET_CONTEXT('jm_ctx', 'part', nvl(l_procdr,l_objct));
  end if;
  l_author := J_MANAGER.GetAuthor(p_job.job_name);
  if l_author is null then 
      debugmsg(0, l_part, 'автором : не определен!', p_job.job_id);
--      l_author := 'SKHIZHNJAK';
  end if;  
  dbms_application_info.set_module(l_author,sys_context('UserEnv','action'));
  dbms_session.SET_CONTEXT('jm_ctx', 'author', l_author);
  j_manager.addp_set(p_job.job_id, 'default', 'last author', l_author);      
                      
  ----------------------------------------------------------------------------
  alter_session(p_job.job_id, 'ENABLE RESUMABLE name', ''''||p_job.job_name||'''');
--  execute immediate 'ALTER SESSION ENABLE RESUMABLE name '''||p_job.job_name||'''';

  ----------------------------------------------------------------------------
  if p_job.maxparallel > 1 then 
    alter_session(p_job.job_id, 'force parallel query parallel', to_char(p_job.maxparallel));
    alter_session(p_job.job_id, 'force parallel DDL parallel', to_char(p_job.maxparallel));
  end if;

  ----------------------------------------------------------------------------
  -- 2013/02/04 - добавление анализа переписывания запросов
  if nvl(addp_get(p_job.job_id, 'default', 'query_rewrite'),'true')<>'false' then 
    alter_session(p_job.job_id, 'set query_rewrite_enabled=',  'true');
    alter_session(p_job.job_id, 'set query_rewrite_integrity=','stale_tolerated');
  else
    j_manager.addp_set(p_job.job_id, 'default', 'last set query_rewrite_enabled=', 'not set');      
  end if;
  l_err := addp_get(p_job.job_id, 'default', 'query_rewrite_integrity');
  if l_err in ('enforsed','trusted','stale_tolerated') then
    alter_session(p_job.job_id, 'set query_rewrite_integrity=',l_err);
  end if;

  ----------------------------------------------------------------------------
  alter_session(p_job.job_id, 'set recyclebin = ', 'on');
  addp_increment(p_job.job_id, 'default', 'run_count_all', 1);
  -- модификатор - блокирует передачу пароля в схемах, которые это предусматривают
  if addp_get(p_job.job_id, 'before', 'dont_send_password') is not null then
    dbms_session.SET_CONTEXT('jm_ctx', 'dont_send_password', '1');
  end if;

  ----------------------------------------------------------------------------
  -- 2012/05/28 устанавливаем NLS параметры для сессии, если они переопределены
  for i in (select * from j_list_add_param where job_id = p_job.job_id and upper(PARAM_NAME) = 'NLS')
  loop
    alter_session(p_job.job_id, 'set ', i.PARAM_VALUE);
  end loop;
  dbms_session.SET_CONTEXT('jm_ctx', 'Reply-To', GetAnswerAble(p_job.job_id));
  j_manager.addp_set(p_job.job_id, 'default', 'last Reply-To', GetAnswerAble(p_job.job_id));      
exception
  when others then -- caution handles all exceptions
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
    debugmsg(-1,l_part, 'Exception:'||l_err, p_job.job_id);
end;


--------------------------------------------------------------------------------
--procedure setJobAfter(p_job in out TJob, p_report in out TReport)
procedure setJobAfter(p_job in out TJob, p_result in number, p_result_txt in varchar2)
is
--  PRAGMA AUTONOMOUS_TRANSACTION;
  l_part      j_log.part%type := 'setJobAfter';
  l_histLevel number;
  l_err       varchar2(3000);
  answ        varchar2(200);
  l_res       number;
  l_sid       number          := sys_context('UserEnv','SID');
  l_spid      number;
  clog        clob;
  exectime    date;
  ctx_job_id  number;
  nls_date    varchar2(100);
  l_damoklov  date;
begin
  execute immediate 'alter session disable parallel query';
  execute immediate 'ALTER SESSION disable PARALLEL DDL';
  
  ---------- вычисляем след.дату запуска
  begin
    if nvl(p_job.nextdat,sysdate)<sysdate then 
      p_job.nextdat := p_job.getnextdate;
    end if;
  exception
    when others then
      l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
      debugmsg(-1,l_part, 'Exception:'||l_err||chr(13)||chr(10)||
                          ' Для отчета '||p_job.job_id||'  '||p_job.job_name||' следующая дата получена с ошибкой. Остановлен.', p_job.job_id);
      p_job.nextdat := null;
  end;
  ---------- сброс даты следующего запуска, если отчет ошибочный
  if p_result <= res_error then
     debugmsg(3, l_part, 'Отчет не успешный. Остановлен.', p_job.job_id);
     p_job.nextdat := null;
  end if;
  debugmsg(3, l_part, 'Дата следующего запуска : '||to_char(p_job.nextdat,'dd.mm.yyyy hh24:mi:ss'), p_job.job_id);

  ------------------------------------------------
  update j_list set (run_by_job, NEXTDAT, param1,       param2,       param3) = 
      (select 0,    p_job.nextdat,  p_job.param1, p_job.param2, p_job.param3 from dual) 
  where job_id = p_job.job_id;

  ------------------------------------------------
  if p_job.job_id <> id_ClearDiedJobs then 
    if p_result <> res_continue then 
      insert into j_list_history(job_id, LastStart, LastEnd , jobRes, jobResTxt, LastMaxSend, server)
        values(p_job.job_id, p_job.laststart, sysdate, p_result, p_result_txt, sysdate, sys_context('USERENV','DB_NAME')||' ('||sys_context('USERENV','DB_UNIQUE_NAME')||') on '||sys_context('USERENV','SERVER_HOST'));
    end if;
    -- удаляем старую историю : остается 3 месяца или столько записей, сколько определено в параметрах
    l_histLevel := GetManagerParamN('JobHistoryCount');
    commit;
    delete from j_list_history 
    where iid in (
        select iid from 
         (
            select iid from 
            ( select iid, 
                     count(iid) over(partition by job_id order by iid desc) iid_potencial, 
                     max(LASTSTART) over(partition by job_id) maxs,
                     row_number()over(partition by job_id order by iid) frst
              from j_list_history
            ) 
            where iid_potencial > l_histLevel and maxs > add_Months(sysdate,-3) and frst <> 1
         ) 
      );
    -- жалуемся ответственному по джобу при ошибках
    if p_result<res_success then
      if p_result = res_continue then 
        debugmsg(3, l_part, 'Отчет перезапустят позднее ('||to_char(p_job.nextdat,'dd.mm.yyyy hh24:mi:ss')||')', p_job.job_id);
        addp_increment(p_job.job_id, 'default', 'delayed_start', 1);
      else
        debugmsg(3, l_part, 'Отчет получили с ошибкой - жалуемся ответственному по джобу', p_job.job_id);
        addp_increment(p_job.job_id, 'default', 'run_count_fail', 1);
      end if;

      answ := GetAnswerAble(p_job.job_id);
      if answ like '%_:%_@_%' then 
        answ := gpv_myEmail;
      end if;
      
      begin 
        select p.spid into l_spid from v$mystat m, v$session s, v$process p where m.sid = s.sid and s.paddr = p.addr and rownum = 1;
      exception  
        when others then null; 
      end;
      ctx_job_id := sys_context('jm_ctx','job_id');
      select value into nls_date from v$nls_parameters where parameter = 'NLS_DATE_FORMAT';
      execute immediate 'alter session set NLS_DATE_FORMAT=''dd/mm/yyyy hh24:mi''';
      clog := get_clob('select * from tlog where sid='||l_sid||' and nvl(spid,0)='||nvl(l_spid,0)||' and nvl(job_id,0)='||nvl(ctx_job_id,0)||' and dinsdate>sysdate-1 order by iid');
      execute immediate 'alter session set NLS_DATE_FORMAT='''||nls_date||'''';
      l_res := emailsender.sendemailwithattach(answ, 
           g_subj||case when p_result=0 then 'Перенос:' else 'Ошибка:' end||p_job.job_name, 
           p_result_txt||chr(13)||chr(10)||'Следующий старт = '||to_char(p_job.nextdat,'dd.mm.yyyy hh24:mi:ss'),
           'Error_protocol_'||case when ctx_job_id is not null then to_char(ctx_job_id) else 'null' end||'.csv',
           clog);
    end if;
  end if; -- if p_job.job_id <> id_ClearDiedJobs then 

  ------------------------------------------------
  if p_job.job_id not in (id_ClearDiedJobs, id_RunOnTestServ) then 
    select count(1) into l_res from j_dispatcher where lower(PARAMNAME) in ('mainserver','backupserver','backupserver2')
           and instr(upper(PARAMVALUE), upper(sys_context('UserEnv','server_host')) )>0;
    if l_res>0 then 
      debugmsg(0,l_part, 'jobid='||p_job.job_id||' -- окончена работа', p_job.job_id);
    end if;
  end if;
  
  ------------------------------------------------
  -- подчищать временные таблицы или нет ?
  begin
     execute immediate 'select '||GetManagerParamV('droptimeout')||' from dual' into l_damoklov;
  exception
     when others then l_damoklov := sysdate+1;
  end;
  if addp_get(p_job.job_id, 'default', 'droptemptab')='true' then 
    dbms_job.submit(l_res, 'j_manager.DropAnyTempTabInThisSess(''DROP%'', '''||sys_context('userenv','SID')||'-'||sys_context('userenv','SESSIONID')||''');', l_damoklov);
  else
      if GetManagerParamB('droptemptab') then 
        dbms_job.submit(l_res, 'j_manager.DropAnyTempTabInThisSess(''DROP%'', '''||sys_context('userenv','SID')||'-'||sys_context('userenv','SESSIONID')||''');', l_damoklov);
      end if;
  end if;
  commit;

  dbms_application_info.set_client_info('');
  dbms_session.clear_all_context('jm_ctx');
exception
  when others then -- caution handles all exceptions
    l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace()||' в джобе '||to_char(p_job.job_id);
    debugmsg(-1,l_part, 'Exception:'||l_err||' {in job_id='||p_job.job_id||'}', p_job.job_id);
    dbms_application_info.set_client_info('');
    dbms_session.clear_all_context('jm_ctx');
end;



--------------------------------------------------------------------------------
function ClearDiedJobs (p_job in out TJob) return TReport
is
  l_part  j_log.part%type := 'ClearDiedJobs';
  l_err   varchar2(5000);
  l_res   number := 0;
  l_cnt   pls_integer :=0;
  cursor c_died is
    select job_id,job_name,run_by_job 
    from j_list m 
    where 
      run_by_job <> 0 
      and (not exists (select 1 from user_jobs where job =m.run_by_job))
      and (not exists (select 1 from user_scheduler_jobs where  comments = to_char(m.run_by_job)))
      and (not exists (select 1 from user_scheduler_jobs where  job_name like 'jm_%'||to_char(m.run_by_job)||'%'))
    for update of run_by_job;
begin
  for i in c_died loop
    debugmsg(0,l_part, 'обновляю - доступной для записи job='||i.job_id||' ('|| i.job_name ||')', p_job.job_id);
    UPDATE j_list SET run_by_job = 0 WHERE CURRENT OF c_died;
  end loop;
  commit;
  return TReport.ReportCreate(res_success, 'Успешно');
exception
  when others then
   l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
   debugmsg(-1,l_part, l_err, p_job.job_id);
   return TReport.ReportCreate(res_error, l_err);
end;

--------------------------------------------------------------------------------
-- Установка джоба на тестовом сервере
FUNCTION RunJobOnTestServ(p_job in out TJOb) RETURN TReport
is
  l_part    j_log.part%type := 'RunJobOnTestServ';
  l_err     varchar2(5000);
  l_cnt     number;
  l_testnum varchar2(40);
begin
  for i in (select * from j_dispatcher where lower(PARAMNAME) like 'dblink_to_test%' and PARAMVALUE is not null)
  loop
    begin
      -- проверяем дб-линк
      if TestLinkAndServer(p_job, i.paramname) then 
          dbms_output.put_line('TestLinkAndServer('||i.paramname||')-success');
          -- получаем номер для тестового сервера
          l_testnum := case when lower(i.paramname)='dblink_to_test' then '1' else trim(substr(i.paramname,15)) end;     
          -- проверяем - есть ли диспетчер на удаленном сервере
          execute immediate 'select count(1) from user_jobs@'||GetManagerParamV(i.paramname)||' where lower(what) like ''%j_manager.dispatch_reports%''' into l_cnt;
          if l_cnt=0 then
            execute immediate '
            declare l_job number;
            begin
              update j_list@'||GetManagerParamV(i.paramname)||' set run_by_job=0 where run_by_job<>0;
              dbms_job.submit@'||GetManagerParamV(i.paramname)||'(l_job, ''begin j_manager.dispatch_reports(NEXT_DATE,''''secondary'''','||l_testnum||'); end;'', sysdate);
              commit;
            end;';
            commit;
            debugmsg(1,l_part, 'Запущен диспетчер на '||GetManagerParamV(i.paramname)||' c номером '||l_testnum, p_job.job_id); 
          end if;
      end if;
    exception
      when others then
        l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
        debugmsg(-1,l_part, 'Exception:'||l_err, p_job.job_id);
    end;
  end loop;
  -- возвращаем успех, чтобы диспетчер продолжал попытки коннекта к тестовой базе
  return TReport.ReportCreate(res_success, 'Успешно');
exception
  when others then
   l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
   debugmsg(-1,l_part, 'Exception:'||l_err, p_job.job_id);
   return TReport.ReportCreate(res_error, l_err);
end;

--------------------------------------------------------------------------------
-- Установка джоба на тестовом сервере
procedure RunJobSaveResult(p_job in out TJOb, res number, res_txt varchar2) 
is
  l_part            j_log.part%type := 'RunJobSaveResult';
  l_err             varchar2(5000);
  l_res             number := 0;
  l_cnt             number;
  l_remSysdate      date;
  l_run_by_job_master   number;
begin
  if p_job.job_id = id_ClearDiedJobs then return; end if;
  if not TestLinkAndServer(p_job, 'dblink_to_main') then 
    return;
  end if;

  execute immediate 'select sysdate from dual@'||GetManagerParamV('dblink_to_main')into l_remSysdate;
  if trunc(sysdate)>trunc(l_remSysdate) and IsStatServ=1 then
    debugmsg(1,l_part, 'Обнаружена работа в будущем на статсервере. Не передаю на боевой laststart, nextdat, param1, param2, param3', p_job.job_id); 
  else
      debugmsg(1,l_part, 'update depstat.j_list@'||GetManagerParamV('dblink_to_main'), p_job.job_id); 
      l_err := 'update depstat.j_list@'||GetManagerParamV('dblink_to_main')||' 
                            set (laststart, nextdat, param1, param2, param3) = 
                                (select '||p_job.LastStart_str||','||p_job.nextdat_str||', '||
                                ''''||replace(p_job.param1,'''','''''')||''', '''||
                                      replace(p_job.param2,'''','''''')||''', '''||
                                      replace(p_job.param3,'''','''''')||'''  from dual)
                            where job_id='||p_job.job_id ;
      execute immediate l_err;
      commit; 
      -- для кикнутого снаружи джоба,закончившегося res_error или res_continue - сбрасываем дату следующего старта
      -- ее (дату) принеобходимости выставит управляющий джоб
      execute immediate 'select run_by_job from j_list@'||GetManagerParamV('dblink_to_main')||' where job_id=:j' into l_run_by_job_master using p_job.job_id;
      if l_run_by_job_master=-1 and res in (res_error, res_continue) then 
        update j_list set nextdat=null where job_id = p_job.job_id;
        debugmsg(1,l_part, 'Обнаружил, что был кикнут и завершился in (res_error, res_comtinue). Сбрасываю локальную дату следующего старта', p_job.job_id); 
        commit;
      end if;
      
  end if;                      
  execute immediate 'update j_list@'||GetManagerParamV('dblink_to_main')||' m
         set (run_by_job, lastjobbody) = (select 0, lastjobbody from j_list where job_id = m.job_id)
         where job_id = '||p_job.job_id;
  commit;                         
  
  debugmsg(1,l_part, 'insert into depstat.j_list_history@'||GetManagerParamV('dblink_to_main'), p_job.job_id); 
  l_err := 'insert into depstat.j_list_history@'||GetManagerParamV('dblink_to_main')||'(job_id, LastStart, LastEnd , jobRes, jobResTxt, LastMaxSend, server)
      values('||to_char(p_job.job_id)       ||', '||
                Date_to_str(p_job.laststart)||', '||
                Date_to_str(sysdate)        ||', '||
                to_char(res)    ||', '||
                ''''||replace(res_txt,'''','"')||''', '||
                'sysdate, '||
                ''''||sys_context('USERENV','DB_NAME')||' ('||sys_context('USERENV','DB_UNIQUE_NAME')||') on '||sys_context('USERENV','SERVER_HOST')||''')';
  execute immediate l_err;
  commit;
  
  debugmsg(1,l_part, 'update depstat.j_recipient@'||GetManagerParamV('dblink_to_main'), p_job.job_id); 
  l_err := 'update depstat.j_recipient@'||GetManagerParamV('dblink_to_main')||' m
       set lastsend = (select max(lastsend) from j_recipient where job_id = m.job_id and recipient = m.recipient)
       where job_id = '||to_char(p_job.job_id);
  execute immediate l_err;
  commit;
  
  debugmsg(0,l_part, 'jobid='||p_job.job_id||' -- окончена работа', p_job.job_id);
exception
  when others then
   l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
   debugmsg(-1,l_part, 'Exception:'||l_err, p_job.job_id);
end;

--------------------------------------------------------------------------------
-- p_job_id   = код джоба, которой необходимо запустить
-- p_srv_name = либо ДБЛИНК к базе где j_list, либо имя параметра из J_DISPATCHER, в котором лежит линк
-- p_start_time - конкретная дата запуска ( поу молчанию -  1 минута)
function kick_job(p_job_id in number, p_server in varchar2 default null, 
  p_param1 in varchar default null, p_param2 in varchar default null, p_param3 in varchar2 default null,
  p_new_start date default sysdate+1/60/24) return number
is
  l_part    j_log.part%type := 'kick_job';
  l_res     number;
  l_run     number;
  l_link    vaRCHAR2(100) := '';
  l_err     varchar2(2000):='';
  l_kiker   number := to_number(nvl(sys_context('jm_ctx','job_id'),0)); --джоб, который пинает другой джоб
  l_kiker_host varchar2(200) := sys_context('USERENV','DB_NAME')||'('||sys_context('USERENV','DB_UNIQUE_NAME')||') on '||sys_context('USERENV','SERVER_HOST');
  l_server  varchar2(100);
begin
  if p_server is not null then 
      if p_server like '@%' then l_link := p_server;
                            else l_link := '@'||GetManagerParamV(p_server);
      end if; 
  end if;
  
  -- проверяем доступность базы
  begin
    execute immediate 'select count(1) from dual'||l_link;
  exception 
    when others then
    begin
      debugmsg(-1,l_part, 'Kick:Ошибка при проверке линка к базе ('||l_link||')', l_kiker);
      debugmsg(-1,l_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace(), l_kiker);
      return -3;
    end;
  end;
    
  -- проверяем наличие джоба на нужном сервере
  begin
    execute immediate 'select 1, nvl(run_by_job,0) from j_list'||l_link||' where job_id=:p_job_id' into l_res, l_run using p_job_id;
  exception
    when others then 
      debugmsg(-1, l_part, 'На сервере ('||l_link||') не обнаружен джоб с номером '||p_job_id, l_kiker);
      debugmsg(-1,l_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace(), l_kiker);
  end;
  -- нет джоба ?
  if l_res is null then 
    debugmsg(-1, l_part, 'На сервере ('||l_link||') не обнаружен джоб с номером '||p_job_id, l_kiker);
    return -1; 
  end if;
  -- джоб работающий
  if l_run > 1 then 
    debugmsg(-1, l_part, 'На сервере ('||l_link||') джоб с номером '||p_job_id||' уже работает', l_kiker);
    return -2; 
  end if; 

  if l_link is null then 
    l_server  := 'onstatserv='||j_manager.getStatServNum||',';
  end if;
  -- можно ставить дату запуска для джоба ? 
  if l_res=1 and l_run=0 then 
    -- запускаем
    execute immediate 'update j_list'||l_link||' 
     set param1=nvl(:m1, param1), 
         param2=nvl(:m2, param2), 
         param3=nvl(:m3, param3), 
         '||l_server||'
         nextdat = '||utils.dat2str(p_new_start)||'
      where job_id = :p_job_id' 
    using p_param1, p_param2, p_param3, p_job_id;
    j_manager.addp_set(l_kiker,     'default','last_kick_'||p_job_id,to_char(sysdate,'dd.mm.yyyy hh24:mi:ss'), l_kiker_host);      
    commit;
    -- протоколируем в логи
    insert into j_log(iDebugLevel, cMsg, part, job_id) values (1, 'Kick job_id='||p_job_id||' на '||l_link||' от '||l_kiker, l_part, l_kiker);
    execute immediate 'insert into j_log'||l_link||'(iDebugLevel, cMsg, part, job_id )
      values(1 , ''Kicked from '||l_kiker_host||' by job_id='||l_kiker||''', '''||l_part||''', '||p_job_id||')';
    commit;
    -- протоколируем в параметры
    j_manager.addp_set(l_kiker, 'default', 'last_kick_'||p_job_id,  to_char(sysdate,'dd.mm.yyyy hh24:mi:ss'),sys_context('USERENV','DB_NAME')||' ('||sys_context('USERENV','DB_UNIQUE_NAME')||') on '||sys_context('USERENV','SERVER_HOST'));      
    execute immediate 'begin j_manager.addp_set'||l_link||'('||p_job_id||', ''default'', ''get_last_kick_from_'||l_kiker||''' , to_char(sysdate,''dd.mm.yyyy hh24:mi:ss''), '''||l_kiker_host||'''); end;';
    if p_server is null then 
      debugmsg(-1,l_part, 'Kick:локальный джоб поставлен на ('||utils.dat2str(p_new_start)||')', p_job_id);
    else
      debugmsg(-1,l_part, 'Kick:удаленный джоб('||p_job_id||') поставлен на('||utils.dat2str(p_new_start)||')', l_kiker);
    end if;
  else
    debugmsg(-1,l_part, 'Kick:удаленный джоб('||p_job_id||') существует, но run_by_job = '||l_res||'. Не ставлю на запуск!', l_kiker);
    return -4;
  end if;

  commit;
  return res_success;
exception
  when others then
   l_err := dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace();
   debugmsg(-1,l_part, 'Exception:'||l_err, p_job_id);
   return sqlerrm;
end;

---------------------------------------------------------------------------------
-- запустить все запланированные и еще не работающие задачи в USER_SCHEDULER_JOBS
procedure push_sch_jobs
is
begin
  for i in (select * from user_scheduler_jobs u 
            where not exists (select 1 from user_scheduler_running_jobs where job_name= u.job_name)
              and u.start_date < sysdate
           )
  loop
    dbms_scheduler.set_attribute(i.job_name,'start_date',sysdate); 
  end loop;
  commit;
end;


--------------------------------------------------------------------------------
procedure addp_increment(p_job_id in number, p_part in varchar2 default 'default', 
    p_paramname in varchar, p_increment in number)
is
  l_part    varchar2(100) := 'addp_increment';
  l_res     j_list_add_param.param_value%type;
  l_resN    number;
begin
  l_res := nvl(addp_get(p_job_id, p_part, p_paramname),0);
  begin
    l_resN := to_number(translate(l_res,', ','.'),'9999999999999999.999999');
    l_resN := l_resN + p_increment;
    addp_set(p_job_id, p_part, p_paramname, l_resN, null);
  exception
    when others then
       debugmsg(-1,l_part, 'не удалось конвертировать '||l_res||' в число',p_job_id);  
  end;
end;

--------------------------------------------------------------------------------
procedure addp_set(p_job_id in number, p_part in varchar2 default 'default', 
    p_paramname in varchar, p_paramvalue in varchar, p_comm varchar default null)
is
  l_part    j_log.part%type := 'addp_set';
  l_comm varchar2(200):= lower(nvl(p_comm, sys_context('USERENV','DB_NAME')||' ('||sys_context('USERENV','DB_UNIQUE_NAME')||') on '||sys_context('USERENV','SERVER_HOST')));
begin
      merge into j_list_add_param m
      using 
        (select p_job_id job_id, 
            lower(p_part) part, 
            substr(lower(p_paramname),1,100) param_name, 
            substr(lower(p_paramvalue),1,100) param_value, 
            lower(l_comm) comm 
          from dual) s
      on 
        (nvl(m.job_id,0) = nvl(s.job_id,0) and m.part = s.part and m.param_name=s.param_name)
      WHEN MATCHED THEN UPDATE SET 
         m.param_value = s.param_value, 
         m.comm = s.comm
      WHEN NOT MATCHED THEN INSERT (m.job_id, m.part, m.param_name, m.param_value, m.comm)
         VALUES (p_job_id, lower(p_part), lower(p_paramname), lower(p_paramvalue), s.comm);
      commit;
  if IsStatServ=1 then 
    execute immediate 'begin j_manager.addp_set'||l_dblMain||'(:pj, :pp, :pn, :pv, :pc); end;' 
        using p_job_id, p_part, p_paramname, p_paramvalue, p_comm; 
  end if;
exception
  when others then
     debugmsg(-1,l_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace(), 0);  
end;

--------------------------------------------------------------------------------
function addp_get(p_job_id in number, p_part in varchar2 default 'default', 
    p_paramname in varchar) return varchar
is
--  l_part    j_log.part%type := 'addp_get';
  res       j_list_add_param.param_value%type;
begin
  select min(lower(param_value)) into res from j_list_add_param 
  where job_id = p_job_id 
    and trim(lower(part)) = trim(lower(p_part))
    and trim(lower(param_name)) = trim(lower(p_paramname));
  commit;
  if res is null then res:=''; end if;

  return res;
end;

---------------------------------------------------------------------------------
procedure DropAnyTempTabInThisSess(p_table_mask in varchar2 default 'DROP%', p_sid_mask in varchar default sys_context('userenv','SID')||'-'||sys_context('userenv','SESSIONID'))
is
  part  varchar2(100) := 'DropAnyTempTabInThisSess';
  cnt   number:=0;
begin
  for i in (select t.owner, t.table_name from sys.all_tables t, sys.all_tab_comments c
        where /*t.owner= sys_context('userenv','CURRENT_USER') and */t.table_name like upper(p_table_mask)
          and t.table_name = c.table_name
          and c.comments like '%s='||p_sid_mask||'%')
  loop
     if cnt=0 then 
       execute immediate 'alter session set recyclebin = on';
       log_ovart(0,1,'Удаление всех временных таблиц в сессии SID='||sys_context('userenv','SID')||', SESSIONID='||sys_context('userenv','SESSIONID')||' по маске '||p_table_mask);
       log_ovart(0,1,'Включена "корзина". Можно попробовать восстановить удаленную таблицу.');
     end if; 
     cnt := cnt+1;
     if i.owner<>sys_context('userenv','SESSION_USER') then 
       log_ovart(0,1,'не могу удалить таблицу '||i.table_name||' в чужой схеме '||i.owner);
     else
       utils.drop_table(/*i.owner||'.'||*/i.table_name, part, false);
     end if;
  end loop;
end;

---------------------------------------------------------------------------------
function GetRecipientList(p_job_id number, p_propocol varchar2 default null) return TStringList pipelined
is
  l_recipient vaRCHAR2(255);
  l_cnt       number :=0;
BEGIN
  for i in (select RECIPIENT from j_recipient r, j_list l
            where r.job_id = p_job_id and r.ENABLED=1 and r.MAXSENDDATE> sysdate and l.SENDONLYANSWERABLE=0
              and r.job_id = l.job_id
            union 
            select RECIPIENT from j_recipient r, j_list l
            where r.job_id = p_job_id and r.ENABLED=1 and r.MAXSENDDATE> sysdate and l.SENDONLYANSWERABLE=1 and r.ANSWERABLE=1
              and r.job_id = l.job_id
           )
  loop
    l_recipient := '';
    if p_propocol is null then 
      l_recipient := i.RECIPIENT; 
    elsif upper(p_propocol) = 'EMAIL' and i.RECIPIENT like '%_@_%._%' then 
      l_recipient := i.RECIPIENT; 
    elsif upper(p_propocol) = 'FTP' and upper(i.RECIPIENT ) like 'FTP://%' then 
      l_recipient := i.RECIPIENT; 
    end if;
    if length(l_recipient)>0 then 
      PIPE ROW(l_recipient);  
      l_cnt := l_cnt +1;
    end if;
  end loop;
  if l_cnt =0  and (p_propocol is null or upper(p_propocol) = 'EMAIL') then 
    PIPE ROW(gpv_myEmail);  
  elsif l_cnt =0  and (p_propocol is null or upper(p_propocol) = 'FTP') then 
    PIPE ROW(gpv_myFTP);
  end if;
END;

---------------------------------------------------------------------------------
BEGIN
  LoadManagerParams(0);
  update j_dispatcher  set PARAMVALUE=to_char(sysdate,'dd.mm.yyyy hh24:mi:ss') where lower(PARAMNAME)='disp_heartbeat';
  commit;
END;
/
