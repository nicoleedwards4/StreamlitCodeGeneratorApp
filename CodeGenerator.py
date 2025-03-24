import streamlit as st
import pandas
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

st.set_page_config(layout='wide')

# Function to get column names from a Snowflake table
def get_column_names(table_name):
    session = get_active_session()
    table = session.table(table_name)
    return [field.name for field in table.schema.fields]

# Function to map Snowflake types to SQL types
def map_snowflake_types_to_sql(snowflake_type):
    mapping = {
        "VARCHAR": "VARCHAR",
        "STRING": "VARCHAR",
        "NUMBER": "NUMBER",
        "FLOAT": "FLOAT",
        "DOUBLE": "FLOAT",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
    }
    return mapping.get(str(snowflake_type).upper(), "VARCHAR")  # Default to VARCHAR

# Helper function to get column names and types from a Snowflake table or view
def get_column_names_and_types(table):
    session = get_active_session()
    df = session.table(table)
    return [(field.name.strip('"'), map_snowflake_types_to_sql(field.datatype)) for field in df.schema.fields]

# Helper function to sanitize column names (removing quotes for display)
def sanitize_column_name(column):
    return column.strip('"')

# Function to generate code based on user inputs
def generate_code(welcome_message, source, target, filter_columns, disable_columns, app_type, form_fields):
    welcome_text = f'st.write(f"Welcome, {{st.experimental_user[\'user_name\']}}!")' if welcome_message else ""
    common_code = f"""
# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from datetime import datetime

# Page Configurations
st.set_page_config(layout='wide')

# Variables for customization
source_table_name = "{source}"
target_table_name = "{target}"
non_editable_columns = {disable_columns}

# Get the current session
session = get_active_session()

{welcome_text}

def get_column_names(table_name):
    table = session.table(table_name)
    return [field.name for field in table.schema.fields]

def generate_insert_statements(df, target_table):
    insert_statements = []
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for _, row in df.iterrows():
        columns = ', '.join([f'"{{col}}"' for col in df.columns] + ['"DATE_MODIFIED"'])
        values = [
            f"NULL" if val is None else f"$${{val}}$$" if isinstance(val, str) else f"'{{val}}'"
            for val in row
        ]
        values.append(f"'{{current_datetime}}'")
        insert_statements.append(f'INSERT INTO {{target_table}} ({{columns}}) VALUES ({{", ".join(values)}});')
    return insert_statements


def update_target_table(df, target_table):
    # Add the USER_ID column with the current user's ID
    df['USER_ID'] = st.experimental_user['user_name']
    insert_statements = generate_insert_statements(df, target_table)
    for stmt in insert_statements:
        session.sql(stmt).collect()
"""

    if app_type == "Table Edit App":
        table_edit_code = f"""
# Function to retrieve distinct values from columns we want to allow as filters on the table
def get_distinct_values(table, column):
    df = session.table(table)
    distinct_values_df = df.select(col(column)).distinct().to_pandas()
    return distinct_values_df[column].tolist()

def get_filtered_data(table, filters):
    df = session.table(table)
    for filter_column, filter_values in filters.items():
        df = df.filter(col(filter_column).isin(filter_values))
    return df.to_pandas()

# Fetch all column names from the source
columns = get_column_names(source_table_name)

# User selects columns for filtering
selected_columns = {filter_columns}

# Create filters based on user selection
filters = {{}}
for column in selected_columns:
    distinct_values = get_distinct_values(source_table_name, column)
    selected_values = st.multiselect(f"Select values for {{column}}", distinct_values)
    if selected_values:
        filters[column] = selected_values

# Fetch filtered data based on selection
if filters:
    filtered_df = get_filtered_data(source_table_name, filters)
else:
    filtered_df = session.table(source_table_name).to_pandas()

# Add "NOTES" and "EXCLUDE" columns to the DataFrame if they don't exist
if 'NOTES' not in filtered_df.columns:
    filtered_df['NOTES'] = ""
if 'EXCLUDE' not in filtered_df.columns:
    filtered_df['EXCLUDE'] = False    

# Display the editable data editor
if non_editable_columns:
    edited_df = st.data_editor(filtered_df, num_rows="dynamic", disabled=non_editable_columns)
else:
    edited_df = st.data_editor(filtered_df, num_rows="dynamic")

# Submit button
submit_button = st.button("Submit")

# Form submission
if submit_button:
    st.write("Form submitted")
    st.write("The following data has been edited and will be written to the target table:")
    st.write(edited_df)
    update_target_table(edited_df, target_table_name)
    st.success(f"Data written to {{target_table_name}}")
"""
        code = common_code + table_edit_code
    elif app_type == "Form Collection App":
        form_collection_code = f"""
# Define the form
with st.form("submission_form"):
    st.write("Please fill out the form")
    form_data = {{}}
    for field, dtype in {form_fields}.items():
        dtype = dtype.lower()
        if dtype == 'text':
            form_data[field] = st.text_input(field)
        elif dtype == 'number':
            form_data[field] = st.number_input(field, step=1)
        elif dtype == 'date':
            form_data[field] = st.date_input(field)
    form_data['USER_ID'] = st.experimental_user['user_name']
    submitted = st.form_submit_button("Submit")

if submitted:
    form_data_df = pd.DataFrame([form_data])
    update_target_table(form_data_df, target_table_name)
    st.success("Form data submitted successfully!")
"""
        code = common_code + form_collection_code
    
    return code

# Streamlit App for User Inputs
st.title("Streamlit Code Generator")

app_type = st.selectbox("Select the type of app", ["Table Edit App", "Form Collection App"])

if app_type == "Table Edit App":
    welcome_message = st.checkbox("Include a welcome message for the user?")
    source = st.text_input("Fully qualified name of the source table or view", help="Example: DATABASE.SCHEMA.TABLE")
    target_suggestion = source + "_EDITED" if source else ""
    target = st.text_input("Fully qualified name of the target table", value=target_suggestion, help="Example: DATABASE.SCHEMA.TABLE")
    columns_with_types = get_column_names_and_types(source) if source else []
    columns = [sanitize_column_name(col) for col, dtype in columns_with_types]
    
    filter_columns = st.multiselect("Select specific columns to filter on (Selecting specific columns will improve performance)", columns)
    disable_columns = st.multiselect("Select columns to disable editing of", columns)
    form_fields = {}

elif app_type == "Form Collection App":
    welcome_message = st.checkbox("Include a welcome message for the user?")
    target = st.text_input("Fully qualified name of the target table", help="Example: DATABASE.SCHEMA.TABLE")
    form_fields = {}
    st.write("Define form fields and types. (Field names must be all caps, with _ instead of spaces)")
    num_fields = st.number_input("Number of fields", min_value=1, step=1)
    for i in range(num_fields):
        raw_field_name = st.text_input(f"Field {i+1} name")
        field_name = raw_field_name.upper().replace(" ", "_")  # Convert to uppercase and replace spaces
    
        with st.expander(f"Options for field {i+1}", expanded=False):
            st.write(f"Final Field Name: `{field_name}`")  # Show the formatted name to the user
            field_dtype = st.selectbox(f"Field {i+1} type", ["Text Entry", "Number", "Date"])
    
            data_type_mapping = {
                "Text Entry": "TEXT",
                "Number": "NUMBER",
                "Date": "DATE"
            }
    
            form_fields[field_name] = data_type_mapping[field_dtype]

    filter_columns = []
    disable_columns = []

# Provide SQL statement for creating the target table first
if app_type == "Table Edit App" and source:
    original_columns = [f'"{col}" {dtype}' for col, dtype in columns_with_types]
    create_table_sql = f"""
-- Run this code in a Snowflake worksheet to create your target table
CREATE OR REPLACE TABLE {target} (
    {", ".join(original_columns)},
    EXCLUDE BOOLEAN,
    USER_ID VARCHAR,
    NOTES STRING,
    DATE_MODIFIED TIMESTAMP_NTZ(9)
);
"""
    st.code(create_table_sql, language="sql")
elif app_type == "Form Collection App":
    form_columns = [f"{name} {dtype.upper()}" for name, dtype in form_fields.items()]
    create_table_sql = f"""
-- Run this code in a Snowflake worksheet to create your target table
CREATE OR REPLACE TABLE {target} (
    {", ".join(form_columns)},
    EXCLUDE BOOLEAN,
    USER_ID VARCHAR,
    NOTES STRING,
    DATE_MODIFIED TIMESTAMP_NTZ(9)
);
"""
    st.code(create_table_sql, language="sql")

# Generate and display the Python code after the SQL
if st.button("Generate Code"):
    if app_type == "Form Collection App":
        code = generate_code(welcome_message, None, target, filter_columns, disable_columns, app_type, form_fields)
    else:
        code = generate_code(welcome_message, source, target, filter_columns, disable_columns, app_type, form_fields)
    st.code(code, language="python")
