# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

import datetime
import logging

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import orm


class DatabaseSession:

    log = logging.getLogger("apimon.DatabaseSession")

    def __init__(self, connection):
        self.connection = connection
        self.session = connection.session

    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        if etype:
            self.session().rollback()
        else:
            self.session().commit()
        self.session().close()
        self.session = None

    def listFilter(self, query, column, value):
        if value is None:
            return query
        if isinstance(value, list) or isinstance(value, tuple):
            return query.filter(column.in_(value))
        return query.filter(column == value)

    def flush(self):
        self.session().flush

    def create_result_task(self, *args, **kw):
        rt = self.connection.ResultTask(*args, **kw)
        self.session().add(rt)
    #    self.session().flush()
        return rt

    def get_result_task(self, job_id, name):

        table = self.connection.ResultTask.__table__

        q = self.session().query(self.connection.ResultTask)

        q = self.listFilter(q, table.c.job_id, job_id)

        q = self.listFilter(q, table.c.name, name)

        try:
            return q.one()
        except orm.exc.NoResultFound:
            return None
        except orm.exc.MultipleResultsFound:
            self.log.error(
                "Multiple ResultTasks found with job_id=%s, name=%s",
                job_id, name)
            return None

    def get_result_summary(self, job_id: str, name: str = None):

        table = self.connection.ResultSummary.__table__

        q = self.session().query(self.connection.ResultSummary)

        q = self.listFilter(q, table.c.job_id, job_id)

        if name:
            q = self.listFilter(q, table.c.name, name)

        try:
            return q.one()
        except orm.exc.NoResultFound:
            return None
        except orm.exc.MultipleResultsFound:
            self.log.error(
                "Multiple ResultSummaries found with job_id=%s, name=%s",
                job_id, name)
            return None

    def create_result_summary(self, *args, **kw):
        rs = self.connection.ResultSummary(*args, **kw)
        self.session().add(rs)
        return rs

    def execute(self, query):
        self.session().execute(query)
        self.session().commit()


class SQLConnection:
    log = logging.getLogger('apimon.SQLConnection')

    def __init__(self, db_url):
        self.db_url = db_url
        self.connected = False

        try:
            self._setup_models()

            self.engine = create_engine(
                self.db_url,
                pool_pre_ping=True
            )
            self.session_factory = orm.sessionmaker(
                bind=self.engine,
                expire_on_commit=False,
                autoflush=False,
            )
            self.session = orm.scoped_session(self.session_factory)

            self.Base.metadata.create_all(self.engine)

            if self.engine.dialect.name == 'postgresql':
                self.engine.execute(self.partition_function)
                self.engine.execute(self.task_trigger)
                self.engine.execute(self.summary_trigger)

            self.connected = True
        except sa.exc.NoSuchModuleError:
            self.log.exception(
                "The required module for the dburi dialect isn't available. "
                "SQL connection will be unavailable.")
        except sa.exc.OperationalError:
            self.log.exception(
                "Unable to connect to the database or establish the required "
                "tables. Reporter %s is disabled" % self)

    def get_session(self):
        return DatabaseSession(self)

    def _setup_models(self):
        Base = declarative_base(metadata=sa.MetaData())

        class ResultTask(Base):
            __tablename__ = 'result_task'
            job_id = sa.Column(sa.String(32), primary_key=True, index=True)
            name = sa.Column(sa.String(255))
            result = sa.Column(sa.Integer)
            duration = sa.Column(sa.Integer)
            timestamp = sa.Column(sa.DateTime, primary_key=True)
            environment = sa.Column(sa.String(255))
            zone = sa.Column(sa.String(32))
            action = sa.Column(sa.String(255))
            play = sa.Column(sa.String(255))
            long_name = sa.Column(sa.String(511))
            state = sa.Column(sa.String(12))
            az = sa.Column(sa.String(32))
            raw_response = sa.Column(sa.Text)
            anonymized_response = sa.Column(sa.Text)

            __table_args__ = (
                sa.Index('result_task_timestamp_idx', 'timestamp'),
                sa.Index('result_task_timestamp_result_idx', 'timestamp',
                         'result'),
                sa.Index('result_task_timestamp_result_env_zone_lname_idx',
                         'timestamp', 'result', 'long_name', 'environment',
                         'zone'),
            )

            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    if k == 'timestamp':
                        self.timestamp = datetime.datetime.fromisoformat(v)
                    elif k != '__type':
                        setattr(self, k, v)
                    if isinstance(v, str) and v[0] == '{':
                        # No use from not expanded vars
                        setattr(self, k, 'N/A')

        class ResultSummary(Base):
            __tablename__ = 'result_summary'
            job_id = sa.Column(sa.String(32), primary_key=True, index=True)
            name = sa.Column(sa.String(255))
            result = sa.Column(sa.Integer)
            duration = sa.Column(sa.Integer)
            timestamp = sa.Column(sa.DateTime, primary_key=True)
            environment = sa.Column(sa.String(255))
            zone = sa.Column(sa.String(32))
            count_passed = sa.Column(sa.Integer)
            count_failed = sa.Column(sa.Integer)
            count_ignored = sa.Column(sa.Integer)
            count_skipped = sa.Column(sa.Integer)

            __table_args__ = (
                sa.Index('result_summary_timestamp_idx', 'timestamp'),
                sa.Index('result_summary_timestamp_result_idx', 'timestamp',
                         'result'),
                sa.Index('result_summary_timestamp_result_name_env_zone_idx',
                         'timestamp', 'result', 'name', 'environment', 'zone'),
            )

            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    if k == 'timestamp':
                        self.timestamp = datetime.datetime.fromisoformat(v)
                    elif k != '__type':
                        setattr(self, k, v)
                    if isinstance(v, str) and v[0] == '{':
                        # No use from not expanded vars
                        setattr(self, k, 'N/A')

        self.Base = Base
        self.ResultTask = ResultTask
        self.ResultSummary = ResultSummary

        self.partition_function = sa.DDL(
            """
CREATE OR REPLACE FUNCTION create_partition_and_insert()
RETURNS trigger AS $BODY$
  DECLARE
    partition TEXT;
    date_from TIMESTAMP;
    date_to TIMESTAMP;
  BEGIN
    partition := TG_RELNAME || '_' || TO_CHAR(NEW.timestamp, 'YYYYMM');
    date_from := date_trunc('MONTH', NEW.timestamp);
    date_to := date_from + '1 month'::interval;

    IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=partition) THEN
      RAISE NOTICE 'A partition has been created';
      EXECUTE 'CREATE TABLE ' || partition || ' (check(timestamp >= DATE ''' ||
            date_from || ''' and timestamp < DATE ''' || date_to ||
            ''')) INHERITS (' || TG_RELNAME || ');';
    END IF;
    EXECUTE 'INSERT INTO ' || partition || ' SELECT(' || TG_RELNAME || ' ' ||
            quote_literal(NEW) || ').* RETURNING job_id;';
    RETURN NULL;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE
            """
        )

        self.task_trigger = sa.DDL(
            """
DROP TRIGGER IF EXISTS result_task_insert_trigger ON result_task;
CREATE TRIGGER result_task_insert_trigger
BEFORE INSERT ON result_task
FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();
            """
        )

        self.summary_trigger = sa.DDL(
            """
DROP TRIGGER IF EXISTS result_summary_insert_trigger ON result_summary;
CREATE TRIGGER result_summary_insert_trigger
BEFORE INSERT ON result_summary
FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();
            """
        )
