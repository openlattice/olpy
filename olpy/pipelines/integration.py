import olpy
import importlib
import pandas as pd
import uuid
import yaml
import re
import subprocess
import shutil
import sqlalchemy
import os
from urllib.parse import unquote
from pkg_resources import resource_filename
import traceback

class Integration(object):
    """
    A class representing an integration configuration
    """

    def __init__(self,
        integration_config = None,
        sql = None,
        file_path = None,
        raw_table_name = "tmp_raw",
        append_raw_table_name = None,
        clean_table_name_root = "tmp",
        standardize_clean_table_name = True,
        if_exists = "fail",
        flight_path = None,
        base_url = "https://api.openlattice.com",
        rowwise = None,
        cleaning_required = True,
        shuttle_path = None,
        drop_table_on_success = False,
        done_folder = None,
        jwt = None):

        # load integration definition

        local_config = dict()
        if isinstance(integration_config, str):
            with open(integration_config, "r") as cf:
                local_config = yaml.load(cf.read(), Loader=yaml.FullLoader)
        elif isinstance(integration_config, dict):
            local_config = integration_config
        elif integration_config is not None:
            raise ValueError("The provided integration_config is not a yaml path or dictionary.")

        self.__dict__.update(local_config)

        with open(resource_filename(__name__, "pipeline_config.yaml"), "r") as gcf:
            global_config = yaml.load(gcf.read(), Loader=yaml.FullLoader)

            # local config supercedes global wherever it overlaps
            
            for k, v in global_config.items():
                if not k in self.__dict__.keys() or not self.__dict__[k]:
                    self.__dict__[k] = v

        
            
        if "sql" in self.__dict__:
            if sql:
                raise ValueError("SQL query specified twice.") 
        else:
            self.sql = sql
        if "file_path" in self.__dict__:
            if file_path:
                raise ValueError("File path specified twice.") 
        else:
            self.file_path = file_path

        if "clean_table_name_root" not in local_config:
            self.clean_table_name_root = clean_table_name_root
        if "standardize_clean_table_name" not in self.__dict__:
            self.standardize_clean_table_name = standardize_clean_table_name
        if "if_exists" not in self.__dict__:
            self.if_exists = if_exists
        if "flight_path" not in self.__dict__:
            self.flight_path = flight_path
        if "base_url" not in self.__dict__:
            self.base_url = base_url
        if "rowwise" not in self.__dict__:
            self.rowwise = rowwise
        if "cleaning_required" not in self.__dict__:
            self.cleaning_required = cleaning_required
        if "shuttle_path" not in self.__dict__:
            self.shuttle_path = shuttle_path
        if "drop_table_on_success" not in self.__dict__:
            self.drop_table_on_success = drop_table_on_success
        if "jwt" not in self.__dict__:
            self.jwt = jwt
        if "raw_table_name" not in local_config:
            self.raw_table_name = raw_table_name
        if "append_raw_table_name" not in local_config:
            self.append_raw_table_name = append_raw_table_name
        if "done_folder" not in local_config:
            self.done_folder = done_folder

        # check completeness of integration definition
        if bool(self.file_path) == bool(self.sql):
            raise ValueError("Exactly one of {file_path, sql} must be specified.")
        if not self.clean_table_name_root:
            raise ValueError("No clean table name specified")
        if not self.flight_path:
            raise ValueError("No flight_path specified")

        # finish setup
        self.configuration = olpy.misc.get_config(jwt = self.jwt, base_url = self.base_url)
        self.flight = olpy.flight.Flight(configuration=self.configuration)
        self.flight.deserialize(self.flight_path)
        self.engine = self.flight.get_atlas_engine_for_organization()

    def clean_row(cls, row):
        raise NotImplementedError("clean_row is not defined for this integration.")

    def clean_df(cls, df):
        raise NotImplementedError("clean_df is not defined for this integration.")
 
    clean_row._unimplemented = True
    clean_df._unimplemented = True

    def determine_rowwise(self):
        self.rowwise = not getattr(self.clean_row, "_unimplemented", False)
        if self.cleaning_required and not self.rowwise and getattr(self.clean_df, "_unimplemented", False):
            print("No cleaning function implemented. Clean table will duplicate raw data.")
            self.cleaning_required = False

    # assume that multiple files can be uploaded at once
    # can either include folder, individual file, or a list of files
    def upload_raw_data_from_filepath(self, connection):
        # if the string is just a direct file, wrap it into a list
        if isinstance(self.file_path, str):
            if os.path.isfile(self.file_path):
                files = [self.file_path]
            # otherwise, it must be a file directory
            else:
                files = []
                for file in os.listdir(self.file_path):
                    # get all json / csv files from this list
                    if file.endswith(".csv") or file.endswith(".json"):
                        files.append(os.path.join(self.file_path, file))
        else:
            files = self.file_path

        df = pd.DataFrame()

        for file in files:
            name, extension = os.path.splitext(file)
            if extension == ".csv":
                df = pd.concat([df, pd.read_csv(file)])
            elif extension == ".json":
                df = pd.concat([df, pd.read_json(file)])
            else:
                print(f"File extension is not supported for {file}.")
                continue

        if len(df.index) > 0:
            # this part might need to be more flexible...
            df.to_sql(
                self.raw_table_name,
                connection,
                if_exists='replace',
                index=False,
                chunksize=1000,
                method='multi'
            )

            if self.append_raw_table_name is not None:
                # append to an all table
                df.to_sql(
                    f'{self.append_raw_table_name}_all',
                    connection,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                    method='multi'
                )

        else:
            raise Exception("Data frame is empty. Nothing to integrate.")

        # include the done folder path
        if self.done_folder is not None:
            if not os.path.exists(self.done_folder):
                os.mkdir(self.done_folder)
                print(f'New directory: {self.done_folder}')

            for fp in files:
                rootdir, file = os.path.split(fp)
                shutil.move(fp, os.path.join(self.done_folder, file))



    def clean_and_upload(self):
        if self.rowwise is None:
            self.determine_rowwise()
        
        if self.standardize_clean_table_name:
            clean_table_name = olpy.clean.atlas.get_temp_table_name(
                self.clean_table_name_root
            )
        else:
            clean_table_name = self.clean_table_name_root


        with self.engine.connect() as connection:

            if self.engine.dialect.has_table(connection, clean_table_name):
                if self.if_exists == "skip":
                    print("Clean table already exists. Skipping the cleaning step.")
                    print(f"{clean_table_name}")
                    return clean_table_name
                if self.if_exists == "replace":
                    print("Clean table already exists. Replacing...")
                    connection.execute(f"drop table {clean_table_name};")
                if self.if_exists == "fail":
                    raise Exception("Clean table name already in use.")

            # TODO parallelize rowwise cleaning
            if self.file_path:
                self.upload_raw_data_from_filepath(connection)
                generator = pd.read_sql_query(
                    f"select * from {self.raw_table_name}",
                    connection,
                    chunksize=1000)
            else:
                generator = pd.read_sql_query(
                    self.sql,
                    connection,
                    chunksize=1000)

            dtypes = self.flight.get_pandas_datatypes_by_column()
            
            rows_cleaned = 0
            rows_fetched = 0
            for chunk in generator:
                rows_fetched += len(chunk)
                cleaned_chunk = chunk
                if self.cleaning_required:
                    if self.rowwise:
                        cleaned_chunk = chunk.apply(self.clean_row, axis=1)
                    else:
                        dummy = self.clean_df(cleaned_chunk)
                        if dummy is not None:
                            cleaned_chunk = dummy
                cleaned_chunk.to_sql(
                    clean_table_name,
                    connection,
                    if_exists='append',
                    dtype={col:(dtypes[col] if col in dtypes.keys() else sqlalchemy.sql.sqltypes.String) for col in cleaned_chunk.columns},
                    index=False,
                    chunksize=1000,
                    method='multi'
                )
                rows_cleaned += len(cleaned_chunk)
                print(f"Fetched and cleaned {rows_fetched} rows. Uploaded a total of {rows_cleaned} rows.")
            if rows_fetched == 0:
                # meaning original dataset was actually empty
                empty = pd.DataFrame(columns = dtypes.keys())
                empty.to_sql(
                    clean_table_name,
                    connection,
                    if_exists='append',
                    dtype=dtypes,
                    index=False
                )
                print(f"Input table is empty ! Uploaded a new empty table.")

        print("Cleaning completed successfully!")
        print(f"{clean_table_name}")
        return clean_table_name


    def integrate_table(self, clean_table_name = None, shuttle_path = None, shuttle_args=None, drop_table_on_success = None, memory_size=None, local=False, sql = None):

        environment = {
            "http://localhost:8080": "LOCAL",
            'https://api.openlattice.com': "PROD_INTEGRATION",
            'https://api.staging.openlattice.com': "STAGING_INTEGRATION"
        }

        host = environment[self.configuration.host]
        if local:
            token = olpy.misc.get_jwt(base_url = "http://localhost:8080")
            host = "LOCAL"
        else:
            token = self.configuration.access_token

        integration_identifier = uuid.uuid4()
        tmp_mapper_path = f'/tmp/mapper_{integration_identifier}.yaml'


        up = re.findall("postgresql://(.*)@", str(self.engine.url))[0]
        up = unquote(up)
        username, password = up.split(":")

        if shuttle_path is None:
            shuttle_path = self.shuttle_path
        if drop_table_on_success is None:
            drop_table_on_success = self.drop_table_on_success

        retval = None

        mapper_dict = {
            'hikariConfigs': {
                str(integration_identifier): {
                    'jdbcUrl': f"jdbc:postgresql://{str(self.engine.url).split('@')[-1]}?ssl=true&sslmode=require",
                    'username': username,
                    "password": password,
                    'maximumPoolSize': 1
                }
            }
        }

        #create temp mapper file to pass to shuttle.
        # will delete on completion or exception.
        with open(tmp_mapper_path, "w") as m:
            yaml.dump(mapper_dict, m)
        
        try:
            if bool(clean_table_name) == bool(sql):
                raise ValueError("Exactly one of {clean_table_name, sql} must be specified.")

            if sql:
                sql = sql.replace('"', '\\"').replace('\\\\"', '\\"')
            else:
                sql = f"select * from {clean_table_name}"

            statement = f'{shuttle_path} --flight {self.flight_path} --token {token} --config {tmp_mapper_path} --datasource {integration_identifier} --sql "{sql}" --environment {host}'

            if shuttle_args:
                statement = f'{statement} {shuttle_args}'

            if memory_size is not None:
                statement = 'SHUTTLE_OPTS="-Xmx{:n}g -Xmx{:n}g" '.format(memory_size, memory_size) + statement

            print(statement.replace(self.configuration.access_token, "***"))

            process = subprocess.Popen(statement, stdout=subprocess.PIPE, shell=True)
            try:
                while process.poll() is None:
                    output = process.stdout.readline()
                    if output:
                        print(output.strip().decode())
                retval = process.poll()
            except Exception as e:
                process.kill()
                raise
            process.kill()
        except Exception as e:
            track = traceback.format_exc()
            print(track)
            os.remove(tmp_mapper_path)
            raise
        os.remove(tmp_mapper_path)

        if retval != 0:
            raise ValueError("The integration did not exit cleanly...")

        print("Integration finished successfully!")
        if drop_table_on_success and clean_table_name:
            self.engine.execute(f"DROP TABLE {clean_table_name};")
            print(f"Dropped table {clean_table_name}")



    def integrate(self, shuttle_path = None, shuttle_args = None, drop_table_on_success = None):
        table = self.clean_and_upload()
        self.integrate_table(
            clean_table_name = table,
            shuttle_path = shuttle_path,
            shuttle_args = shuttle_args,
            drop_table_on_success = drop_table_on_success
        )





