@when('I enter the following Oracle credentials')
def step(context):
    for row in context.table:
        user = row['User']
        password = row['Password']
        host = row['Host']
        port = row['Port']
        service_name = row['Service_name']
    context.oracleDB = oracle_db.OracleDB(user,password,host,port,service_name)

@then('I should see a successful connection message')
def step(context):
    assert context.oracleDB.connected

@then("we select ALL_TABLES")
def step_impl(context):
    rows = context.oracleDB.select(
        "ALL_TABLES",
        {"key": "TABLE_NAME", "value": "SYSTEM_PRIVILEGE_MAP"},
        column_name = ["TABLE_NAME"]
    )
    assert len(rows) > 0
    schema = {
        "type": "array",
        "items":  {
            "type": "object",
            "properties":{
                "TABLE_NAME": {"type": "string"},
            },
            "required": [
                "TABLE_NAME"
            ]
        }
    }
    validate(instance=rows, schema=schema)

@then("we select_by_query ALL_TABLES")
def step_impl(context):
    rows = context.oracleDB.select_by_query(
        "ALL_TABLES",
        "TABLE_NAME LIKE '%PRIVILEGE%'",
        column_name = ["TABLE_NAME"]
    )
    assert len(rows) > 0
    schema = {
        "type": "array",
        "items":  {
            "type": "object",
            "properties":{
                "TABLE_NAME": {"type": "string"},
            },
            "required": [
                "TABLE_NAME"
            ]
        }
    }
    validate(instance=rows, schema=schema)

@then('we create a table named "{table_name}"')
def step_impl(context, table_name):
    columns = [
        {"key": key, "value": value} for key, value in context.table[0].items()
    ]
    context.oracleDB.create_table(table_name, columns)
    assert context.oracleDB.table_exists(table_name)

@then('we delete_table "{table_name}"')
def step_impl(context, table_name):
    context.oracleDB.delete_table(table_name)

@then('we insert some data into table "{table_name}" using values parameter')
def step_impl(context, table_name):
    for row in context.table.rows:
        value = [
            {'key': key, 'value': value} for key, value in row.items()
        ]
        context.oracleDB.add_data(table_name, value)

@then('we insert some data into table "{table_name}" using kwargs')
def step_impl(context, table_name):
    for row in context.table.rows:
        value = {key: value for key, value in row.items()}
        context.oracleDB.add_data(table_name, **value)

@then('we remove_data from table "{table_name}"')
def step_impl(context, table_name):
    condition = {key: value for key, value in context.table[0].items()}
    assert context.oracleDB.remove_data(table_name, **condition)

@then('we check that table "{table_name}" has "{count}" records')
def step_impl(context, table_name, count):
    assert context.oracleDB.count_records(table_name) == int(count)

@then('we remove_all_data from table "{table_name}"')
def step_impl(context, table_name):
    assert context.oracleDB.remove_all_data(table_name)

@then('we add_data_batch')
def step_impl(context):
    data_dict = {
        table_name: file_name
        for table_name, file_name in context.table[0].items()
    }
    context.oracleDB.add_data_batch(data_dict)

@then('we close the connection')
def step_impl(context):
    context.oracleDB.close()

@then('we update_data "{table_name}"')
def step_impl(context, table_name):
    values_to_update = {"POWER": 140}
    conditions = {"BRAND": "Toyota", "MODEL": "Camry"}
    context.oracleDB.update_data(table_name, values_to_update, **conditions)
    rows = context.oracleDB.select(
        "CARS",
        column_name = ["*"]
    )
    logging.info("New update table:")
    logging.info(rows)
