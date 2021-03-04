
import itertools

import warnings

from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

warnings.filterwarnings("ignore")

# search column on the schemas you are looking for or across the database
def column_search(spark_session, col, schema='all'):
    main_list = []

    col = col.lower()

    no_permission = []

    kudu_table = []

    if schema == 'all':

        schemas = spark_session.sql("show databases").toPandas()

        schemas = list(itertools.chain(*schemas.values))

    elif isinstance(schema, str) and schema != 'all':

        schemas = spark_session.sql(f"show databases like '{schema}*'").toPandas()

        schemas = list(itertools.chain(*schemas.values))

    elif isinstance(schema, list):

        if len(schema) > 0:

            schemas = []

            for i in schema:
                schemass = spark_session.sql(f"show databases like '{i}*'").toPandas()

                schemass = list(itertools.chain(*schemass.values))

                schemas = schemas + schemass

    if len(schemas) > 0:

        for sch in schemas:

            try:

                tables = spark_session.sql(f"show tables in {sch}").toPandas()

                tables = [i[1] for i in tables.values]

            except Exception as e:

                no_permission.append(sch)

                tables = []

            if len(tables) > 0:

                for tab in tables:

                    sub_list = []

                    try:

                        columns = spark_session.sql(f"show columns in {sch}.{tab}").toPandas()

                        columns = list(itertools.chain(*columns.values))

                    except Exception as e:

                        kudu_table.append(tab)

                        columns = []

                    if len(columns) > 0:

                        if len([i for i in columns if col in i]) > 0:
                            sub_list.append(sch)

                            sub_list.append(tab)

                            sub_list.append([i for i in columns if col in i])

                            main_list.append(sub_list)

    print("\n")

    if len(no_permission) > 0:
        print("The schemas you may not have permission on are {}".format({(', '.join(sl for sl in no_permission))}))

    print("\n")

    if len(kudu_table) > 0:
        print("The kudu tables cannot be searched are {}".format({(', '.join(sl for sl in kudu_table))}))

    print("\n")

    if len(main_list) > 0:

        for i in main_list:
            print("The columns found in the schema '{}' under the table '{}' are {}".format(i[0], i[1], {
                (', '.join(sl for sl in i[2]))}))



# months forward and back to create features based on the current date


def dates_generator(date, column, backward, forward=0):

    main_list = []

    today = datetime.strptime(date, '%Y-%m-%d').date()

    if isinstance(backward, int):

        first = today.replace(day=1)

        for i in range(backward):
            sub_list = []

            lastMonthe = first - timedelta(days=1)

            lastMonthb = lastMonthe.replace(day=1)

            first = lastMonthe.replace(day=1)

            sub_list.append(str(lastMonthb))

            sub_list.append(str(lastMonthe))

            sub_list.append(column + str('_b') + str(i + 1))

            main_list.append(sub_list)

    if isinstance(forward, int):

        first = today.replace(day=1)

        for i in range(forward):
            sub_list = []

            nextMonthb = first + relativedelta(months=1)

            nextMonthe = nextMonthb + relativedelta(day=31)

            first = nextMonthe.replace(day=1)

            sub_list.append(str(nextMonthb))

            sub_list.append(str(nextMonthe))

            sub_list.append(column + str('_f') + str(i + 1))

            main_list.append(sub_list)

    return main_list