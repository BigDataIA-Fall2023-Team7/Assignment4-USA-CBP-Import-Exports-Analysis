import streamlit as st
import re
import warnings
from snowflake.snowpark.exceptions import SnowparkSQLException
from langchain.llms import OpenAI
from langchain.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.chat_models import ChatOpenAI
from langchain.chains import create_sql_query_chain
from utils.snowchat_ui import StreamlitUICallbackHandler, message_func

warnings.filterwarnings("ignore")

INITIAL_MESSAGE = [
    {"role": "user", "content": "Hi!"},
    {
        "role": "assistant",
        "content": "Hello! I am Snowflake ChatBot, your SQL-speaking and ready to query Snowflake to get answers! ❄️🔍",
    },
]

# Initialize session state
if "messages" not in st.session_state:
    st.session_state["messages"] = INITIAL_MESSAGE
if "history" not in st.session_state:
    st.session_state["history"] = []

st.title("❄️ Snowflake SQL ChatBot ❄️")

# Prompt for user input and save
if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})

for message in st.session_state.messages:
    message_func(
        message["content"],
        True if message["role"] == "user" else False,
        True if message["role"] == "data" else False,
    )

callback_handler = StreamlitUICallbackHandler()

# Snowflake connection details
snowflake_url = f"snowflake://{st.secrets.user}:{st.secrets.password}@{st.secrets.account}/{st.secrets.database}/{st.secrets.schema}?warehouse={st.secrets.warehouse}&role={st.secrets.role}"

# Create SQLDatabase instance
conn = SQLDatabase.from_uri(
    snowflake_url,
    sample_rows_in_table_info=3,
    schema=st.secrets.schema, ## ADI: ADDED the schema and schema_artifacts from secrets.toml
    include_tables=st.secrets.schema_artifacts, ## ADI: ADDED the schema and schema_artifacts from secrets.toml
    view_support=True,
)

st.success("Connected to Snowflake!")

chain = create_sql_query_chain(ChatOpenAI(temperature=0), conn)


def append_chat_history(question, answer):
    st.session_state["history"].append((question, answer))


def get_sql(text):
    sql_match = re.search(r"```sql\n(.*)\n```", text, re.DOTALL)
    return sql_match.group(1) if sql_match else None


def append_message(content, role="assistant", display=False):
    message = {"role": role, "content": content}
    st.session_state.messages.append(message)
    if role != "data":
        append_chat_history(st.session_state.messages[-2]["content"], content)

    if callback_handler.has_streaming_ended:
        callback_handler.has_streaming_ended = False
        return


def handle_sql_exception(query, conn, e, retries=2):
    append_message("Uh oh, I made an error, let me try to fix it..")
    error_message = (
        "You gave me a wrong SQL. FIX The SQL query by searching the schema definition:  \n```sql\n"
        + query
        + "\n```\n Error message: \n "
        + str(e)
    )
    new_query = chain({"question": error_message})  # , "chat_history": ""})["answer"]
    append_message(new_query)
    if get_sql(new_query) and retries > 0:
        return execute_sql(get_sql(new_query), conn, retries - 1)
    else:
        append_message("I'm sorry, I couldn't fix the error. Please try again.")
        return None


def execute_sql(result, conn, retries=2):
    if re.match(r"^\s*(drop|alter|truncate|delete|insert|update)\s", result, re.I):
        append_message("Sorry, I can't execute queries that can modify the database.")
        return None
    try:
        return conn.run(result)
    except SnowparkSQLException as e:
        return handle_sql_exception(result, conn, e, retries)


if st.session_state.get("messages") and st.session_state["messages"] and st.session_state["messages"][-1]["role"] != "assistant":
    content = st.session_state["messages"][-1]["content"]
    with st.chat_message("assistant"):
        result = chain.invoke({"question": content})
        st.markdown(f"**Generated SQL Query:**\n```sql\n{result}\n```")
    # Append the result only if it hasn't been appended before
    if not any(msg["content"] == result for msg in st.session_state["messages"]):
        append_message(result)

# Add a Run Query button only if there is a SQL query and the button has been clicked
if st.session_state.get("messages") and st.session_state["messages"]:
    content = st.session_state["messages"][-1]["content"]
    result = chain.invoke({"question": content})
    if result is not None and st.button("Run Query"):
        st.markdown(f"**Generated SQL Query:**\n```sql\n{result}\n```")
        # Append the result only if it hasn't been appended before
        if not any(msg["content"] == result for msg in st.session_state["messages"]):
            append_message(result)

        # Execute the query only if the button has been clicked
        df = execute_sql(result, conn)
        if df is not None:
            callback_handler.display_dataframe(df)
            append_message(df, "data", True)

# Add a reset button
if "messages" in st.session_state.keys() and st.session_state["messages"] != INITIAL_MESSAGE:
    if st.button("Reset Chat"):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.session_state["messages"] = INITIAL_MESSAGE
        st.session_state["history"] = []
