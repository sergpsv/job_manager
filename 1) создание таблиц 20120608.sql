-- Start of DDL Script for Table DEPSTAT.J_DISPATCHER
-- Generated 8/06/2012 14:33:04 from DEPSTAT@BISDB


----------------------------------------------------------------------------------------------------
-- общие условия работы
----------------------------------------------------------------------------------------------------
CREATE SEQUENCE job_sequence
  INCREMENT BY 1
  START WITH 1
  MINVALUE 1
  MAXVALUE 999999999999999999999999999
  NOCYCLE
  NOORDER
  CACHE 20
/



----------------------------------------------------------------------------------------------------
-- параметры для диспетчера
CREATE TABLE j_dispatcher
   (paramname                      VARCHAR2(20),
    paramvalue                     VARCHAR2(100),
    defaultval                     VARCHAR2(100),
    changed                        NUMBER DEFAULT 1,
    description                    VARCHAR2(150))
  PCTFREE     10
/

-- Grants for Table
GRANT DELETE ON j_dispatcher TO sparshukov WITH GRANT OPTION;
GRANT INSERT ON j_dispatcher TO sparshukov WITH GRANT OPTION;
GRANT UPDATE ON j_dispatcher TO sparshukov WITH GRANT OPTION;


-- Triggers for J_DISPATCHER
CREATE OR REPLACE TRIGGER j_dispatcher_ch
 BEFORE
  INSERT OR UPDATE
 ON j_dispatcher
REFERENCING NEW AS NEW OLD AS OLD
 FOR EACH ROW
begin
  if :new.paramvalue <> :old.paramvalue then :new.changed:=1; end if;
end;
/
-- Comments for J_DISPATCHER
COMMENT ON TABLE j_dispatcher IS 'Настройки для пакета J_MANAGER'
/

----------------------------------------------------------------------------------------------------
-- список отчетов
CREATE TABLE j_list
   (job_id                         NUMBER NOT NULL,
    job_name                       VARCHAR2(100) NOT NULL,
    nextdat                        DATE,
    nextformula                    VARCHAR2(1000),
    maxnextdat                     DATE DEFAULT to_date('31/12/2999','dd/mm/yyyy'),
    param1                         VARCHAR2(100),
    param2                         VARCHAR2(100),
    param3                         VARCHAR2(100),
    enabled                        NUMBER DEFAULT 0,
    onstatserv                     NUMBER DEFAULT 0,
    sendonlyanswerable             NUMBER DEFAULT 0,
    laststart                      DATE,
    run_by_job                     NUMBER DEFAULT 0,
    maxparallel                    NUMBER DEFAULT 0,
    job_comment                    VARCHAR2(500),
    lastjobbody                    CLOB,
    navi_date                      DATE DEFAULT sysdate)
  PCTFREE     1
/
-- Grants for Table
GRANT DELETE ON j_list TO sparshukov WITH GRANT OPTION;
GRANT INSERT ON j_list TO sparshukov WITH GRANT OPTION;
GRANT SELECT ON j_list TO sparshukov;
GRANT UPDATE ON j_list TO sparshukov WITH GRANT OPTION;
-- Constraints for J_LIST
ALTER TABLE j_list ADD CONSTRAINT j_list_pk PRIMARY KEY (job_id) USING INDEX PCTFREE     10
/
-- Comments for J_LIST
COMMENT ON TABLE j_list IS 'Список программ, которые нужно запускать в то или иное время. Джоб можно запускать, если (RUN_BY_JOB = 0) и (nextdat < текущей) и (enabled = 1)';
COMMENT ON COLUMN j_list.enabled IS '1-разрешен или 0-нет';
COMMENT ON COLUMN j_list.job_comment IS 'Комментарий';
COMMENT ON COLUMN j_list.job_id IS 'код программы';
COMMENT ON COLUMN j_list.job_name IS 'название';
COMMENT ON COLUMN j_list.lastjobbody IS 'тело джоба поставленного на выполнение в lastStart';
COMMENT ON COLUMN j_list.laststart IS 'дата последнего запуска';
COMMENT ON COLUMN j_list.maxnextdat IS 'предельная дата до которой осуществляется расчет';
COMMENT ON COLUMN j_list.maxparallel IS 'максимальное кол-во паралельностей для джоба но не выше заданного лимита в J_dispatcher';
COMMENT ON COLUMN j_list.navi_date IS 'дата внесения записи в j_list';
COMMENT ON COLUMN j_list.nextdat IS 'Дата следующего старта';
COMMENT ON COLUMN j_list.nextformula IS 'строка типа trunc(:nextdat)+4/24';
COMMENT ON COLUMN j_list.onstatserv IS '0-на боевом, 1 и выше - на статистическом. цифра определяется параметром из Dispatch_reports';
COMMENT ON COLUMN j_list.param1 IS 'параметр 1';
COMMENT ON COLUMN j_list.param2 IS 'параметр 2';
COMMENT ON COLUMN j_list.param3 IS 'параметр 3';
COMMENT ON COLUMN j_list.run_by_job IS 'ID job''a запустившегося на работу';
COMMENT ON COLUMN j_list.sendonlyanswerable IS 'Отправка только ответственному по отчету';



CREATE TABLE j_list_add_param
    (job_id                         NUMBER,
    part                           VARCHAR2(20),
    param_name                     VARCHAR2(100),
    param_value                    VARCHAR2(100),
    comm                           VARCHAR2(300),
    last_update                    DATE)
/
-- Indexes for J_LIST_ADD_PARAM
CREATE UNIQUE INDEX j_list_add_param_ui ON j_list_add_param
  (
    job_id                          ASC,
    part                            ASC,
    param_name                      ASC
  )
  PCTFREE     10
/
-- Constraints for J_LIST_ADD_PARAM
ALTER TABLE j_list_add_param ADD CONSTRAINT j_list_add_param_part_chk CHECK (lower(part) in ('default','before','after'))
/
-- Triggers for J_LIST_ADD_PARAM
CREATE OR REPLACE TRIGGER j_list_add_param_lu
 BEFORE
  INSERT OR UPDATE
 ON j_list_add_param
REFERENCING NEW AS NEW OLD AS OLD
 FOR EACH ROW
begin
  :new.last_update := sysdate;
end;
/
-- Comments for J_LIST_ADD_PARAM
COMMENT ON TABLE j_list_add_param IS 'os_user=sparshukov, author=, info='
/
-- Foreign Key
ALTER TABLE j_list_add_param ADD CONSTRAINT j_list_add_param_fk FOREIGN KEY (job_id) REFERENCES j_list (job_id)
/

CREATE TABLE j_list_history
    (job_id                         NUMBER NOT NULL,
    laststart                      DATE,
    lastend                        DATE,
    jobres                         NUMBER,
    jobrestxt                      VARCHAR2(1000),
    lastmaxsend                    DATE,
    iid                            NUMBER,
    server                         VARCHAR2(200))
  PCTFREE     10
/



-- Triggers for J_LIST_HISTORY
CREATE OR REPLACE TRIGGER j_list_history_iid
 BEFORE
  INSERT
 ON j_list_history
REFERENCING NEW AS NEW OLD AS OLD
 FOR EACH ROW
begin
  select job_sequence.nextval into :new.iid from dual;
end;
/


-- Comments for J_LIST_HISTORY
COMMENT ON TABLE j_list_history IS 'История работы джоба:последние несколько записей о его работе';
COMMENT ON COLUMN j_list_history.jobres IS 'Результат работы (код)';
COMMENT ON COLUMN j_list_history.jobrestxt IS 'Результат работы (расшифровка)';
COMMENT ON COLUMN j_list_history.lastend IS 'Дата окончания работы джоба';
COMMENT ON COLUMN j_list_history.lastmaxsend IS 'максимальная дата из тех, на кого был отправлен этот отчет';
COMMENT ON COLUMN j_list_history.laststart IS 'Дата запуска джоба';


-- Foreign Key
ALTER TABLE j_list_history ADD CONSTRAINT j_list_history_job_id_fk FOREIGN KEY (job_id) REFERENCES j_list (job_id)
/
CREATE TABLE j_log
   (dinsdate                       DATE DEFAULT sysdate,
    idebuglevel                    NUMBER(*,0) DEFAULT 0,
    cmsg                           VARCHAR2(100),
    iid                            NUMBER(*,0),
    part                           VARCHAR2(20),
    job_id                         NUMBER)
  PCTFREE     1
/

-- Indexes for J_LOG
CREATE INDEX j_log_date_level ON j_log
  (
    dinsdate                        ASC,
    idebuglevel                     ASC
  )
  PCTFREE     10
/

-- Triggers for J_LOG
CREATE OR REPLACE TRIGGER jlog_iid
 BEFORE
  INSERT
 ON j_log
REFERENCING NEW AS NEW OLD AS OLD
 FOR EACH ROW
begin
  select idanytable_seq.nextval into :new.iid from dual;
end;
/


-- Comments for J_LOG
COMMENT ON TABLE j_log IS 'протокол работы процедур пакета JOBs'
/

CREATE TABLE j_pass
   (dinsdate                       DATE DEFAULT sysdate,
    job_id                         NUMBER DEFAULT sys_context('jm_ctx','job_id'),
    filename                       VARCHAR2(200),
    pass                           VARCHAR2(200),
    author                         VARCHAR2(100) DEFAULT nvl(sys_context('jm_ctx','author'),sys_context('userEnv','os_user')))
  PCTFREE     10
/
-- Comments for J_PASS
COMMENT ON TABLE j_pass IS 'Таблица с паролями, которые были применены при отправке отчетов'
/


CREATE TABLE j_recipient
   (job_id                         NUMBER NOT NULL,
    recipient                      VARCHAR2(200) NOT NULL,
    enabled                        NUMBER NOT NULL,
    answerable                     NUMBER DEFAULT 0,
    lastsend                       DATE,
    maxsenddate                    DATE DEFAULT to_date('31/12/2999','dd/mm/yyyy'),
    sms_daypart                    NUMBER,
    navi_date                      DATE DEFAULT sysdate,
    navi_user                      VARCHAR2(100) DEFAULT sys_context('userenv','os_user'))
  PCTFREE     10
/

-- Grants for Table
GRANT DELETE ON j_recipient TO sparshukov WITH GRANT OPTION;
GRANT INSERT ON j_recipient TO sparshukov WITH GRANT OPTION;
GRANT UPDATE ON j_recipient TO sparshukov WITH GRANT OPTION;


-- Constraints for J_RECIPIENT
ALTER TABLE j_recipient ADD CONSTRAINT j_recipient_answ CHECK (answerable in (1,0)) ;
ALTER TABLE j_recipient ADD CONSTRAINT j_recipient_enabled CHECK (enabled in (1,0));


-- Comments for J_RECIPIENT
COMMENT ON TABLE j_recipient IS 'Список получателей отчета';
COMMENT ON COLUMN j_recipient.answerable IS '1-ответственный';
COMMENT ON COLUMN j_recipient.enabled IS '1-разрешена доставка, 0- запрещена';
COMMENT ON COLUMN j_recipient.job_id IS 'job_id из таблицы j_list';
COMMENT ON COLUMN j_recipient.lastsend IS 'Дата последней отправки';
COMMENT ON COLUMN j_recipient.maxsenddate IS 'После этой даты не доставлять';
COMMENT ON COLUMN j_recipient.recipient IS 'получатель отчета';

-- Foreign Key
ALTER TABLE j_recipient ADD CONSTRAINT j_recipient_job_id_fk FOREIGN KEY (job_id) REFERENCES j_list (job_id)
/


CREATE TABLE j_recipient_sms
   (recipient                      VARCHAR2(200),
    enabled                        NUMBER DEFAULT 1,
    msisdn                         VARCHAR2(50),
    daypart                        NUMBER DEFAULT 7, -- 1 утро, 2 день, 4 вечер, 8 ночь,
    navi_date                      DATE DEFAULT sysdate
    )
  PCTFREE     0
/


-- Indexes for J_RECIPIENT_SMS
CREATE UNIQUE INDEX j_recipient_sms_rec ON j_recipient_sms
  (
    recipient                       ASC
  )
  PCTFREE     10
/


-- Constraints for J_RECIPIENT_SMS
ALTER TABLE j_recipient_sms ADD CONSTRAINT j_recipient_sms_days CHECK (daypart <= 15)
/

-- Triggers for J_RECIPIENT_SMS
CREATE OR REPLACE TRIGGER j_recipient_sms_trg
 BEFORE
  INSERT OR UPDATE
 ON j_recipient_sms
REFERENCING NEW AS NEW OLD AS OLD
 FOR EACH ROW
begin
  :new.RECIPIENT := lower(:new.RECIPIENT);
end;
/


-- Comments for J_RECIPIENT_SMS
COMMENT ON TABLE j_recipient_sms IS 'номера и маски суток для смс информирования';
COMMENT ON COLUMN j_recipient_sms.daypart IS 'общая маска : часть суток для попытки отправки: 1 утро, 2 день, 4 вечер, 8 ночь';
COMMENT ON COLUMN j_recipient_sms.enabled IS 'можно\нельзя отправлять';
COMMENT ON COLUMN j_recipient_sms.msisdn IS 'номер - весь номер важен';
COMMENT ON COLUMN j_recipient_sms.recipient IS 'мыло получателя из j_recipient';

-- End of DDL Script for Table DEPSTAT.J_RECIPIENT_SMS

