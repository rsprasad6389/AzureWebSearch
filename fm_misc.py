# import streamlit as st
import pandas as pd
global prompt
import pandas as pd
from io import StringIO
import io
from dotenv import load_dotenv
load_dotenv()
import matplotlib.pyplot as plt
# import snowflake.connector as sf
# from snowflake.snowpark import Session
import os
import ssl
import certifi
import uuid
import sys
# from langchain.schema import HumanMessage
# from langchain.schema import HumanMessage

# from langchain.prompts import PromptTemplate
from langchain_core.prompts import PromptTemplate
import matplotlib.pyplot as plt

# from langchain.chains import LLMChain

import pandas as pd
import os
# import ollama
# from langchain.prompts import PromptTemplate
from langchain.tools import tool
# from langchain.agents import Tool

# from langchain.chains import LLMChain
# from langchain_ollama import ChatOllama
# from langchain.callbacks import StreamlitCallbackHandler
from langchain_community.callbacks.streamlit import StreamlitCallbackHandler

from langchain_openai import AzureChatOpenAI
import snowflake.connector as sf
from snowflake.snowpark import Session
from langchain_core.output_parsers import StrOutputParser
import re
from datetime import datetime
from typing import Tuple
from openai import OpenAI  # or AzureOpenAI, or use LangChain LLM interface
import pandas as pd
from pathlib import Path
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent

# from langgraph.prebuilt import create_react_agent
# from langchain.agents import AgentExecutor
# from langchain.agents import AgentExecutor
# from langchain.agents import initialize_agent,AgentType
# from langchain.memory import ConversationBufferMemory
distinct_values = {}

flag_column = 0


df_activities2 = pd.DataFrame()
df_final = pd.DataFrame()
df_contact = pd.DataFrame()
df_all_oppts = pd.DataFrame()
df = pd.DataFrame()
st.set_page_config(page_title="FM AI Agent", layout="wide")







# Create a function to execute and cache the queries


llm = AzureChatOpenAI(
                                api_key = os.getenv("AZURE_OPENAI_API_KEY"),
                                azure_endpoint = os.getenv("AZURE_ENDPOINT"),
                        
                                azure_deployment = os.getenv("LLM"),
                               
                                api_version = os.getenv("API_VERSION"),
                                temperature = 1
                                
                                )


vision_llm =  AzureChatOpenAI(
                                api_key = os.getenv("AZURE_OPENAI_API_KEY"),
                                azure_endpoint =  os.getenv("AZURE_ENDPOINT"),
                                model = os.getenv("LLM_VISION"),
                                api_version=os.getenv("API_VERSION_VISION"),
                                temperature = 0.
                                )



user = os.getenv("USER")
account=os.getenv("ACCOUNT")
password =os.getenv("PASSWORD")
warehouse = os.getenv("WAREHOUSE")
database =os.getenv("DATABASE")

df_grouped = ''
df_activities = pd.DataFrame()
df_oppt_details = pd.DataFrame()




# #change username here
def snowpark_sso_connection():
    connection_parameters = {    
        "ACCOUNT":account,
#         "USER": "rsprasad@beckman.com",
        "USER":user,
        "PASSWORD" :password,
    
        "WAREHOUSE" : warehouse,
        "DATABASE" :database,
#         "SCHEMA" :"SFDC"
        
        
    }
    session = Session.builder.configs(connection_parameters).create()
    return session
session = snowpark_sso_connection()

def normalize_result(result):
    """
    Normalize any LLM-produced result into:
    - rows: list[dict] OR None
    - columns: list[str] OR None
    - explanation: str OR None
    """

    # ----------------------------
    # Pandas DataFrame
    # ----------------------------
    if isinstance(result, pd.DataFrame):
        df = result.copy()

        # flatten tuple column names
        df.columns = [
            "_".join(map(str, c)) if isinstance(c, tuple) else str(c)
            for c in df.columns
        ]

        return df.to_dict("records"), list(df.columns), None
    # ----------------------------
    # List of dicts (table-like)
    # ----------------------------
    if isinstance(result, list):
        if len(result) == 0:
            return [], [], None

        if all(isinstance(r, dict) for r in result):
            columns = list(result[0].keys())
            return result, columns, None

        # list of scalars → wrap
        return (
            [{"value": r} for r in result],
            ["value"],
            None
        )

    # ----------------------------
    # Dict
    # ----------------------------
    if isinstance(result, dict):
        # table-like dict
        if all(isinstance(v, (list, tuple)) for v in result.values()):
            df = pd.DataFrame(result)
            return (
                df.to_dict(orient="records"),
                list(df.columns),
                None
            )

        # key-value explanation
        return (
            [{"key": k, "value": v} for k, v in result.items()],
            ["key", "value"],
            None
        )

    # ----------------------------
    # Scalar (int, float, bool)
    # ----------------------------
    if isinstance(result, (int, float, bool)):
        return (
            [{"value": result}],
            ["value"],
            f"Computed value: {result}"
        )

    # ----------------------------
    # String
    # ----------------------------
    if isinstance(result, str):
        return (
            None,
            None,
            result
        )

    # ----------------------------
    # Fallback
    # ----------------------------
    return (
        [{"value": str(result)}],
        ["value"],
        str(result)
    )

def clean_code(llm_output: str) -> str:
    """
    Safely extract Python code from an LLM response.
    Handles:
    - ```python ... ```
    - ``` ... ```
    - plain code
    - mixed explanation + code
    """

    text = llm_output.strip()

    # Case 1: wrapped in ```python ... ```
    if "```python" in text:
        return text.split("```python")[1].split("```")[0].strip()

    # Case 2: wrapped in ``` ... ```
    if text.startswith("```") and text.endswith("```"):
        return text.strip("`").strip()

    # Case 3: plain code
    return text


def get_reports(df_rls, manager):
    """Return all employees under this manager (direct + indirect)."""
    result = set()
    stack = [manager]

    while stack:
        m = stack.pop()
        # st.write('M is..', m)
        emps = df_rls[df_rls["MANAGER E-MAIL ADDRESS"] == m]["EMPLOYEE EMAIL ADDRESS"].tolist()
        # st.write('Employee list is..', emps)
        for e in emps:
            if e not in result:
                result.add(e)
                stack.append(e)

    print('List is..', result)
    return list(result)


def apply_rls(df_rls, user):
    """Return only the rows the user is allowed to see."""
    # If user is not a manager → return only their row
    # st.write(df["manager_id"].unique())
    # user = int(user)
    # df["manager_id"] = df["manager_id"].astype(str)
    # st.write(type(df.loc[0,'manager_id']))
    if user not in df_rls["MANAGER E-MAIL ADDRESS"].unique():
        return [user]

    # If user is manager → include user + all their reports
    reports = get_reports(df_rls, user)
    allowed = [user] + reports
    return allowed




def FM_Misc_fn(query: str, email:str, flag:int) -> str:
    


    """
    Perform analysis on the global dataframe `df` 
    supplied and writes and executes Python code to give answers for monthwise, yearwise and leadsource wise performance of an opportunity owner or an email in terms of funnel
    creation.

    Args:
        query (str): The search query.
        

    Returns:
        str: Result of the code execution (without extra commentary)
    """
    # get_data()
    print('I am in FM_Misc tool and email is', email)
    # keywords = extract_keywords(st.session_state['prompt'])
    global df
    df = pd.read_pickle('df_misc_funnel.pkl')
    # df_rls = pd.read_pickle(BASE_DIR/'rls.pkl')
    
    # print(relevant_email_list)

    
    global distinct_values

    for col in df.columns:
        unique_vals = df[col].dropna().astype(str).str.strip().str.lower().unique().tolist()
        # Keep only short, meaningful values (optional)
        unique_vals = [v for v in unique_vals]
        distinct_values[col] = unique_vals[:10]

    
    prompt_template = """



You are an expert in writing Python code and executing it.

Question is: "{question}".
You have a global dataframe 'df'.

If the user’s question asks for a “pie chart”, you must draw a pie chart, not a bar graph.


You are writing Python code that will be executed via exec() in an isolated environment.

MANDATORY RULES:
MANDATORY RULES:
1. Do NOT reference variables from outer scope inside functions or lambdas.
2. Every function must receive all required inputs as parameters OR bind them using default arguments.

4. Callback functions (matplotlib, pandas apply, etc.) must be self-contained.
5. All outputs must be assigned to a variable named `result`.

7. Code must be safe to re-run multiple times.
3. ALL functions must be defined BEFORE they are called.
   NEVER reference a function before defining it.

4. All function definitions must be at the top level of the code.
   NEVER define a function inside an if/else block or inside another function.

5. The code must include ALL helper functions it uses — do NOT assume any functions exist already.

6. If the code uses a variable, it MUST be defined earlier in the script.
   Never reference undefined variables.

7. Never print anything. Never include explanation text. 
   Return ONLY valid executable Python code.

8. The final output of the script MUST assign to a variable named:
      result
   If a chart is created, assign the matplotlib figure to:
      fig

9. NEVER define standalone variables, constants, or dictionaries.
   All mappings must be:
   - inside a function, OR
   - passed as a default argument to a function.

10. If a mapping is required, wrap it in a function.

STRICT REQUIREMENTS:
*** 
Never ever change the case of any column in global dataframe 'df'. 
Never import streamlit and write streamlit related code and never write st.pyplot(fig)
All the column names are in lower case already — keep them as is.
Just return the code with no description or comments.
***

ADDITIONAL RULES FOR DATA TYPES:
- For integer or float columns (like 'funnel'), NEVER compare them to quoted strings.
- For varchar columns (like 'year'), compare them to quoted strings.
  Example: use (df['year'] == '2025')
- For string columns, always compare in lowercase using `.str.lower()`.
  Example: (df['2024 legal entity 3'].str.lower() == 'north america').
- Always respect the data types listed below. Do not assume or change them.

Below are the columns with their meaning and possible distinct values 
(only use a column if the keyword or phrase from the {question}
matches one of its values. Convert keyword in lower letters):

{distinct_values}

Column Name	Description /Data type/  Meaning
"account name" - name of the account
"opportunity id" - id of the opportunity 
"contact name" - name of the contact/customer
"contact email" - email of the contact/customer
"stage of oppt" - stage of the opportunity 
"amount of opportunity" - value of the opportunity
"reason won/lost" - reason as to why an opportunity was won or lost
"product in the opportunity" - product associated with the opportunity
"lead source" - str - The original or detailed source of the lead (Web, Event, Partner, Referral).
"opco" - Danaher operating company 

ANALYSIS RULES:
- Ignore case differences in both dataframes and in user questions.
- Never add any new filters — all necessary filters are already applied in 'df_oppts_yearwise'.
- When performing a comparison, also compute and include the subtraction value in `result`.
- Sort the months (if applicable) as '1','2','3','4','5','6','7','8','9','10','11','12'.
- If you want a specific column (say, "year") to become the columns after .unstack(),
  make sure "year" is the second (inner) key in your groupby:
  e.g. df.groupby(['country', 'year'])['funnel'].sum().unstack()
  or explicitly:
  df.groupby(['year', 'country'])['funnel'].sum().unstack(level='year')

STRICT REQUIREMENTS:
***
- Always use the data given only in the dataframe 'df'


- Always identify the chart type explicitly from the question using the following strict priority order:

    1. If the question explicitly contains the words "pie chart" or "pie", draw a pie chart.
    2. If the question explicitly contains the words "bar chart", "bar graph", or "bar", draw a bar chart.
    3. If the question explicitly contains the words "line chart", "line graph", or "trend", draw a line chart.
    4. If the question contains the words "share" or "proportion" but does NOT explicitly mention "bar" or "line",
       then draw a pie chart.
    5. Otherwise, default to a vertical bar chart.

- NEVER choose a pie chart if the question explicitly mentions "bar" or "line",
  even if it also includes words like "share", "distribution", "percentage", or "proportion".

- Before plotting, if 'month' is used on the x-axis, convert it to integer:
    df['month'] = df['month'].astype(int)
    df = df.sort_values('month')

For questions related to quarter, calculate quarter
AND 
- While using ceiling function, always use numpy ceil
e.g 
import numpy as np
df_2025['quarter'] = np.ceil(df_2025['created date'].dt.month / 3).astype(int)
Never ever use st.pyplot(fig)
- For pie charts:
    * Use matplotlib’s `ax.pie()` function.
    * Include percentage labels (`autopct='%1.1f%%'`).
    * Always include a descriptive title.
    * Example:
        fig, ax = plt.subplots()
        data = df.groupby('high geo')['funnel'].sum()
        ax.pie(data, labels=data.index, autopct='%1.1f%%', startangle=90)
        ax.set_title('Share of Funnel by High Geo for 2025')
        


- For bar or line charts:
    * Use matplotlib syntax:
        fig, ax = plt.subplots()
        ... # plotting code
       
    * NEVER use plt.show().
    * Tilt x-axis labels by 90 degrees for readability.


-- If a chart is created, assign the matplotlib Figure object to a variable named `fig`

--- When drawing bar graph, write the number on top of the respective bar.

- In case of plotting a graph, put legend outside the graph.
- DO NOT save the figure to disk
- DO NOT choose filenames
- The runtime will handle rendering and persistence



- Whenever the question includes words like "compare", "trend", "difference", "vs", "over time", or "between", ALWAYS:
    1. Aggregate data properly before comparison.
    2. When grouping by year, month, or any other field, always flatten the grouped dataframe so it has a single-level column structure.
        - Example:
            df_2025_grouped = df_2025.groupby('month')['funnel'].sum().rename('2025')
            df_2024_grouped = df_2024.groupby('month')['funnel'].sum().rename('2024')
            df_combined = pd.concat([df_2025_grouped, df_2024_grouped], axis=1)
            df_combined['difference'] = df_combined['2025'] - df_combined['2024']
        - DO NOT use `.unstack()` or create multi-index columns.
    3. Sort by month (1 to 12) if applicable.
    4. Include both the numeric result (in `result`) 


- Assign both:
    1. The numeric or tabular output to a variable named `result`


- For questions related to account names or account ids:
    * Use groupby as below:
      result = df_filtered.groupby(['year', 'account name'])['funnel'].sum().unstack('year')  # for account names
      result = df_filtered.groupby(['year', 'account id 18'])['funnel'].sum().unstack('year')  # for account ids


- Every variable used must be defined before use
- Do not reference variables defined inside conditional blocks unless guaranteed to exist

-If a groupby is used, ALWAYS call `.reset_index()` before assigning `result`


***

OUTPUT FORMAT:
- Do not print anything else except valid Python code.
- Assign the final computed value to `result`.***

You are FunnelGPT — a sales and marketing funnel analysis expert.
"""



    template = PromptTemplate(
    input_variables=[ "question", "distinct_values"],
    template=prompt_template,
    )

    llm_chain = template| llm 



    # ---------------------------
    # Streamlit App Interface
    # ---------------------------








    print('Query is..', query)
    # Ask the user for a question about the data
    question = query
    # while True:
    if question:
       

        print('Writing code..')
        attempt = 0
        if (True):
            
            global result
            while attempt <3 and True:
                try:
                
                    
                    print(f'Attempt number is..{attempt}')
                    # Run the LLMChain to generate the Python script based on the question and CSV columns
                    python_script = llm_chain.invoke({
                        
                        "question": question,
                        "distinct_values":distinct_values
                        
                        
                    })
                    # print('Python script is..', python_script)
                    # print(type(python_script))
                
                    # print('Text of Python script is..', python_script.content)
                    code = python_script.content
                    code = clean_code(python_script.content)

                    print('Code is..', code)
                    


                    import numpy as np
                    exec_env = {
                    "df": df,
                    "pd": pd,
                    "np": np,
                    "plt": plt,
                }
                    print("Executing code..")
                    exec(code, exec_env, exec_env)
                    print("Executed code..")

                    if "result" not in  exec_env:
                        attempt += 1
                    else:
                        
                        result =  exec_env["result"]
                        fig =  exec_env.get("fig")

                        stage_path = None
                        print("fig is..", fig)
                        if fig is not None:
                            import uuid, tempfile, os

                            # 1️⃣ Save locally (Azure-safe)
                            tmp_dir = tempfile.gettempdir()
                            filename = f"{uuid.uuid4()}.png"
                            local_path = os.path.join(tmp_dir, filename)

                            fig.savefig(local_path, bbox_inches="tight")

                            # 2️⃣ Upload to Snowflake stage
                            stage_path = f"@FM_GPT.CHAT_IMAGES_STAGE/{filename}"

                            session.file.put(
                                local_path,
                                stage_path,
                                overwrite=True,
                                auto_compress=False
                            )

                            # Optional cleanup
                            try:
                                os.remove(local_path)
                            except Exception:
                                pass

                        print("Returning stuff..")
                        rows, cols, explanation = normalize_result(result)
                        return {
                            "status": "ok",
                            "result": rows,
                            "columns":cols,
                            "chart_stage_path": stage_path,
                            "code" : code
                        }
                except Exception as e:
                    # st.error(f"🚫 Error generating the code 2: {e}")
                    print((f"🚫 Error generating the code 2: {e}"))
                    attempt += 1
                        # return('Error')




@tool(return_direct=True)
def FM_Misc_Tool(query: str, email: str, flag:int):
    """
IMPORTANT: Use this tool for ALL questions related to funnel analysis, including:
- Funnel comparison
- Funnel aggregation
- Funnel breakdown by any dimension such as:
  - 
  - Lead source
  - Account
  
- Any funnel-related chart (bar, pie, line)

DO NOT use this tool for:

- details of a contact email or contact name
-
    """
 
    print("Email received:", email)
    return FM_Misc_fn(query, email, flag)
