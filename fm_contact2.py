import pandas as pd
import os
import numpy as np
from pathlib import Path
from langchain.tools import tool
from langchain_core.prompts import PromptTemplate
from langchain_openai import AzureChatOpenAI
import matplotlib.pyplot as plt
from snowflake.snowpark import Session
import streamlit as st
import uuid
import tempfile

BASE_DIR = Path(__file__).resolve().parent

# ---------------------------------------
# 1️⃣ Load LLM
# ---------------------------------------
llm = AzureChatOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_ENDPOINT"),
    azure_deployment=os.getenv("LLM"),
    api_version=os.getenv("API_VERSION"),
    temperature=1
)

# ---------------------------------------
# 2️⃣ Snowflake Session
# ---------------------------------------
def snowpark_connect():
    connection_parameters = {
        "ACCOUNT": os.getenv("ACCOUNT"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "WAREHOUSE": os.getenv("WAREHOUSE"),
        "DATABASE": os.getenv("DATABASE"),
    }
    return Session.builder.configs(connection_parameters).create()

session = snowpark_connect()

# ---------------------------------------
# 3️⃣ Load Summary Table Once
# ---------------------------------------
def load_contact_summary():
    df = pd.read_pickle('df_misc_funnel.pkl')
    df.columns = [c.lower() for c in df.columns]
    return df

df_contact = load_contact_summary()

# ---------------------------------------
# 4️⃣ Helper: Normalize results
# ---------------------------------------
def normalize_result(result):
    if isinstance(result, pd.DataFrame):
        return result.to_dict("records"), list(result.columns), None
    if isinstance(result, (int, float, str)):
        return [{"value": result}], ["value"], None
    if isinstance(result, list):
        return [{"value": v} for v in result], ["value"], None
    return [{"value": str(result)}], ["value"], None

# ---------------------------------------
# 5️⃣ Code extraction
# ---------------------------------------
def clean_code(text: str):
    if "```python" in text:
        return text.split("```python")[1].split("```")[0].strip()
    if text.startswith("```") and text.endswith("```"):
        return text.strip("`").strip()
    return text.strip()

# ---------------------------------------
# 6️⃣ MAIN LOGIC - Executes analysis on df_contact
# ---------------------------------------
def ContactAgent_fn(query: str):
    global df_contact
    print('I am in contact agent')
    prompt_template = """
You are ContactGPT — an expert analyst of contact opportunity history.

You have a pandas dataframe called `df_contact` with columns:
- contact id
- total_oppts
- open_oppts
- closed_oppts
- total_amount_won
- total_amount_lost
- win_rate_contact
- oppt_history (JSON array)

======================================================
BEHAVIOR RULES (CRITICAL)
======================================================

### 1️⃣ SUMMARY MODE (CONTACT DETAILS MODE)

If the user question contains:
- "details of contact"
- "summary of contact"
- "tell me about contact"
- "explain contact"
- "full details"
- "contact overview"
- "show opportunities for contact"

THEN:

✔ DO NOT create human-readable summaries in Python  
✔ DO NOT loop through oppt_history  
✔ DO NOT build text descriptions  

INSTEAD PYTHON MUST:

1. Filter df_contact for the contact_id (case-insensitive)
2. Extract:
      - row_data = row with all columns
      - oppt_json = the JSON list from row["oppt_history"]
3. Assign:
      result = row_data (as a DataFrame or dict)
      oppt_json = oppt_json list
4. DO NOT create charts
5. DO NOT compute metrics
6. DO NOT print anything

The LLM outside Python will generate the text summary.

### 2️⃣ ANALYSIS MODE (WRITE PYTHON)

If question is ANY other type of analytical query:
- Use df_contact
- Write proper Python code
- Assign final output to result
- If chart created, assign to fig
- Make comparisons case-insensitive

======================================================
GENERAL RULES
======================================================
- Output ONLY Python code.
- Use pandas, numpy, matplotlib only.
- result must always exist.
- Never import streamlit.

Question:
{question}

Write ONLY Python code below.
"""


    template = PromptTemplate(
        input_variables=["question"],
        template=prompt_template,
    )

    chain = template | llm

    # Ask LLM to produce Python code
    python_script = chain.invoke({"question": query})
    code = clean_code(python_script.content)

    import numpy as np
    exec_env = {"df_contact": df_contact, "pd": pd, "np": np, "plt": plt}

    # Run the produced code
    exec(code, exec_env, exec_env)

    # Extract results
    result = exec_env.get("result", None)
    fig = exec_env.get("fig", None)
    oppt_json = exec_env.get("oppt_json", None)
    print(oppt_json)

    chart_stage_path = None
    # if fig is not None:
    #     tmp_dir = tempfile.gettempdir()
    #     filename = f"{uuid.uuid4()}.png"
    #     local_path = os.path.join(tmp_dir, filename)

    #     fig.savefig(local_path, bbox_inches="tight")

    #     stage_path = f"@FM_GPT.CHAT_IMAGES_STAGE/{filename}"
    #     session.file.put(local_path, stage_path, overwrite=True, auto_compress=False)
    #     chart_stage_path = stage_path

    rows, cols, explanation = normalize_result(result)

    return {
    "status": "ok",
    "result": rows,
    "columns": cols,
    "chart_stage_path": chart_stage_path,
    "code": code
}


# ---------------------------------------
# 7️⃣ Expose as a LangChain Tool
# ---------------------------------------
@tool(return_direct=True)
def FM_Contact_Tool(query: str):
    """
Use this tool for ALL questions related to:
- contact details or details of an email id
- total opportunities
- open/closed opportunities
- won/lost amounts
- product history
- opportunity history for a contact
- what products each contact bought
- what is the total revenue from a contact
- show all lost opportunities for a contact
    """
    return ContactAgent_fn(query)
