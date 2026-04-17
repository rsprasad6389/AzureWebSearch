# ==========================================================
# fm.py — FINAL, STAGE-BASED, HISTORY-SAFE VERSION
# ==========================================================
import hashlib
import os
import json
import uuid
import tempfile
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from snowflake.snowpark import Session
from snowflake.snowpark.functions import parse_json

from langchain_openai import AzureChatOpenAI
from langchain.agents import create_agent
from langchain_core.messages import ToolMessage

from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient

from fm_misc import FM_Misc_Tool
from fm_contact2 import FM_Contact_Tool

import os
import json
import re
from tavily import TavilyClient
from firecrawl import Firecrawl
from langchain_openai import AzureChatOpenAI
import requests
# from bs4 import BeautifulSoup

TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")

tavily = TavilyClient(api_key=TAVILY_API_KEY)
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)


email = st.text_input('Enter email').strip().lower()
print('Email is..', email)
exception_email = ['rsprasad@beckman.com', 'ldmello@beckman.com',\
                   'pbopp@beckman.com']
flag = 0
# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------
st.set_page_config(page_title="Funnel Management GPT", layout="wide")
st.title("📊 Funnel Management GPT")
st.caption("Type your question below or use one of the sample questions as inspiration 👇")

st.markdown("### 💡 Example Questions")
canned_questions = [
    "Compare win rate across different months for 2025 with 2024",
    "Show MQL to SQL ratio for different high geos for 2025 in a table with sql count, mql count and sql/mql ratio",
    "Show funnel velocity won across different obi groups for 2025",
    "Show the total open weighted funnel closing in this quarter in 2025 across different high geos",
    "Sort campaigns in North America by sql/mql ratio for 2025 and show in a table campaign name, sql count, mql count and sql/mql ratio",
    "Compare the funnel created across different high geos in 2025 by a pie chart"
]

for q in canned_questions:
    st.markdown(f"🔹 *{q}*")




IMAGE_STAGE = "@FM_GPT.CHAT_IMAGES_STAGE"

# ----------------------------------------------------------
# ENV
# ----------------------------------------------------------
ACCOUNT   = os.getenv("ACCOUNT")
USER      = os.getenv("USER")
PASSWORD  = os.getenv("PASSWORD")
WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE  = os.getenv("DATABASE")

# ----------------------------------------------------------
# LLM
# ----------------------------------------------------------
llm = AzureChatOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_ENDPOINT"),
    azure_deployment=os.getenv("LLM"),
    api_version=os.getenv("API_VERSION"),
    temperature=1,
)

# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------

# Load Secrets
# TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
# FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")

# tavily = TavilyClient(api_key=TAVILY_API_KEY)
# firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)
# BING_API_KEY = os.getenv("BING_API_KEY")
# BING_ENDPOINT = os.getenv(
#     "BING_ENDPOINT",
#     "https://api.bing.microsoft.com/v7.0/search"
# )

# def crawl_url(url: str):
#     """
#     Uses Firecrawl to extract markdown text from a URL.
#     Returns clean markdown text.
#     """
#     try:
#         result = firecrawl.scrape(
#             url=url,
#             formats=["markdown"]
#         )
#         return result.markdown or ""
#     except:
#         return ""

def azure_abm_websearch(name, company, country, question="Find publicly available business info"):
    try:
        project = AIProjectClient(
            endpoint=os.getenv("AZURE_PROJECT_ENDPOINT"),
            credential=DefaultAzureCredential()
        )

        agent_id = os.getenv("AZURE_AGENT_ID")

        thread = project.agents.threads.create()

        prompt = f"""
        Person: {name}
        Company: {company}
        Country: {country}

        {question}

        Return:
        - Public profile summary
        - Likely projects / initiatives
        - Recent company news
        - Business priorities
        - Useful sales insights
        Only use public web sources.
        """

        project.agents.messages.create(
            thread_id=thread.id,
            role="user",
            content=prompt
        )

        run = project.agents.runs.create_and_process(
            thread_id=thread.id,
            agent_id=agent_id
        )

        messages = project.agents.messages.list(thread_id=thread.id)

        final_text = ""
        for msg in messages:
            if msg.role == "assistant":
                for part in msg.content:
                    if hasattr(part, "text"):
                        final_text += part.text.value + "\n"

        return final_text.strip()

    except Exception as e:
        return f"Web search failed: {str(e)}"

def crawl_url(url: str):
    """
    Uses Firecrawl to extract markdown text from a URL.
    Returns clean markdown text.
    """
    try:
        result = firecrawl.scrape(
            url=url,
            formats=["markdown"]
        )
        return result.markdown or ""
    except:
        return ""

def web_search(query: str, num_results=5):
    """
    Searches using Tavily and extracts top URLs + snippets.
    """
    try:
        results = tavily.search(
            query=query,
            max_results=num_results,
            include_raw_content=True
        ).get("results", [])

        cleaned = []
        for r in results:
            cleaned.append({
                "url": r.get("url", ""),
                "snippet": r.get("content", "")[:500]
            })
        return cleaned
    except:
        return []

# def web_search(query: str, num_results=5):
#     """
#     Search internet using Azure Bing Search
#     """
#     try:
#         headers = {
#             "Ocp-Apim-Subscription-Key": BING_API_KEY
#         }

#         params = {
#             "q": query,
#             "count": num_results,
#             "textDecorations": True,
#             "textFormat": "Raw"
#         }

#         response = requests.get(
#             BING_ENDPOINT,
#             headers=headers,
#             params=params,
#             timeout=15
#         )

#         data = response.json()

#         results = []

#         for item in data.get("webPages", {}).get("value", []):
#             results.append({
#                 "url": item.get("url", ""),
#                 "snippet": item.get("snippet", "")
#             })

#         print(results)
#         st.write('Results are ..', results)
#         return results

#     except Exception as e:
#         print("Bing Search Error:", e)
#         return []
    

def simple_web_agent(company: str, country: str, question="latest news"):
    """
    Simple combined search + crawl + LLM summary agent.
    """

    search_query = f"{company} {country} {question}"
    tavily_results = web_search(search_query, num_results=2)

    crawled_pages = []

    for item in tavily_results:
        st.write('Url is..', item['url'])
        url = item["url"]
        markdown = crawl_url(url)
        crawled_pages.append({
            "url": url,
            "snippet": item["snippet"],
            "markdown": markdown[:5000]   # Limit to avoid token explosion
        })
        # st.write(item["snippet"])
        # st.write(markdown[:5000])
    # Final LLM summary
    summary_prompt = f"""
Summarize publicly available information for:


Company: {company}
Country: {country}

Here are search + crawl results:

{json.dumps(crawled_pages, indent=2)}

Write a clean, factual, easy-to-understand summary including:
- Recent news
- Professional background (if found)
- Company context
- Market signals
- Any risks or alerts
- Anything useful for a sales conversation

Do NOT guess anything. Use only provided text.
"""

    summary = llm.invoke(summary_prompt).content
    # st.write('Web summary is..', summary)
    return {
        "summary": summary,
        "search_results": tavily_results,
        "raw_pages": crawled_pages
    }



def execution_key(prompt, conversation_id):

    
    raw = f"{conversation_id}::{prompt.strip()}"
    return hashlib.md5(raw.encode()).hexdigest()

def generate_chat_title(prompt, max_words=7, max_chars=60):
    clean = " ".join(prompt.strip().split())
    words = clean.split()
    return " ".join(words[:max_words])[:max_chars]

def get_session():
    return Session.builder.configs({
        "ACCOUNT": ACCOUNT,
        "USER": USER,
        "PASSWORD": PASSWORD,
        "WAREHOUSE": WAREHOUSE,
        "DATABASE": DATABASE,
    }).create()

session = get_session()

##########################
#GET DATA
@st.cache_data
def load_data():
    

    df = pd.read_excel("ABM_Data.xlsx", sheet_name = 'Common')

    # Convert the results to DataFrame if needed
    df.columns = df.columns.str.lower()

  
    # keywords = extract_keywords(st.session_state['prompt'])

    df.to_pickle('df_misc_funnel.pkl')





def parse_tool_output(content):
    """
    Safely parse ToolMessage.content into a Python dict.
    Handles dict, JSON string, python-literal string.
    """

    if content is None:
        return None

    # Already parsed
    if isinstance(content, dict):
        return content

    # JSON string
    if isinstance(content, str):
        content = content.strip()

        # Try JSON first
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Try Python literal (single quotes etc.)
        try:
            import ast
            return ast.literal_eval(content)
        except Exception:
            pass

    raise ValueError(f"Unparseable tool output: {content}")


# ----------------------------------------------------------
# STATE
# ----------------------------------------------------------
def init_state():
    if "conversation_id" not in st.session_state:
        st.session_state.conversation_id = None
    if "messages" not in st.session_state:
        st.session_state.messages = []

# ----------------------------------------------------------
# IMAGE RENDER
# ----------------------------------------------------------
def render_image_from_stage(stage_path: str):
    tmp_dir = tempfile.mkdtemp()
    session.file.get(stage_path, tmp_dir)
    local_path = os.path.join(tmp_dir, os.path.basename(stage_path))
    st.image(local_path)

# ----------------------------------------------------------
# DB PERSISTENCE
# ----------------------------------------------------------
def save_turn(session, cid, role, content):
    tid = str(uuid.uuid4())

    session.sql("""
        INSERT INTO FM_GPT.CHAT_TURNS
        (CONVERSATION_ID, TURN_ID, ROLE, CONTENT, USER_EMAIL, CREATED_AT)
        VALUES (?, ?, ?, ?,?, CURRENT_TIMESTAMP)
    """, params=[cid, tid, role, content, email]).collect()

    session.sql("""
        UPDATE FM_GPT.CHAT_CONVERSATIONS
        SET UPDATED_AT = CURRENT_TIMESTAMP
        WHERE CONVERSATION_ID = ?
    """, params=[cid]).collect()

    return tid

def store_table_artifact(session, cid, tid, df):
    payload = json.dumps(df.to_dict("records"))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    session.create_dataframe(
        [[cid, tid, "TABLE", payload, created_at, email]],
        schema=[
            "CONVERSATION_ID",
            "TURN_ID",
            "ARTIFACT_TYPE",
            "ARTIFACT_DATA",
            "CREATED_AT",
            "USER_EMAIL"
        ],
    ).select(
        "CONVERSATION_ID",
        "TURN_ID",
        "ARTIFACT_TYPE",
        parse_json("ARTIFACT_DATA").alias("ARTIFACT_DATA"),
        "CREATED_AT",
        "USER_EMAIL",
    ).write.mode("append").save_as_table("FM_GPT.CHAT_ARTIFACTS")




def store_code_artifact(session, cid, tid, code):
    payload = json.dumps({"code": code})
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    session.create_dataframe(
        [[cid, tid, "CODE", payload, created_at, email]],
        schema=[
            "CONVERSATION_ID",
            "TURN_ID",
            "ARTIFACT_TYPE",
            "ARTIFACT_DATA",
            "CREATED_AT",
            "USER_EMAIL"
        ],
    ).select(
        "CONVERSATION_ID",
        "TURN_ID",
        "ARTIFACT_TYPE",
       
        parse_json("ARTIFACT_DATA").alias("ARTIFACT_DATA"),
        "CREATED_AT",
        "USER_EMAIL"
    ).write.mode("append").save_as_table("FM_GPT.CHAT_ARTIFACTS")

def store_image_ref(session, cid, tid, stage_path):
    payload = json.dumps({"stage_path": stage_path})
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    session.create_dataframe(
        [[cid, tid, "IMAGE_REF", payload, created_at, email]],
        schema=[
            "CONVERSATION_ID",
            "TURN_ID",
            "ARTIFACT_TYPE",
            "ARTIFACT_DATA", 
            "CREATED_AT",
            "USER_EMAIL"
        ],
    ).select(
        "CONVERSATION_ID",
        "TURN_ID",
        "ARTIFACT_TYPE",
        parse_json("ARTIFACT_DATA").alias("ARTIFACT_DATA"),
        "CREATED_AT",
        "USER_EMAIL"
    ).write.mode("append").save_as_table("FM_GPT.CHAT_ARTIFACTS")

# ----------------------------------------------------------
# LOAD HISTORY
# ----------------------------------------------------------
def load_conversation(session, conversation_id, email):

    st.session_state.conversation_id = conversation_id
    st.session_state.messages = []

    turns = session.sql("""
        SELECT TURN_ID, ROLE, CONTENT
        FROM FM_GPT.CHAT_TURNS
        WHERE CONVERSATION_ID = ?
        AND USER_EMAIL = ?
        ORDER BY CREATED_AT
    """, params=[conversation_id, email]).collect()

    for t in turns:
        turn_id = t["TURN_ID"]

        # User message
        if t["ROLE"] == "user":
            st.session_state.messages.append({
                "role": "user",
                "type": "text",
                "content": t["CONTENT"],
            })
            continue

        # Assistant message — load artifacts
        artifacts = session.sql("""
            SELECT ARTIFACT_TYPE, ARTIFACT_DATA
            FROM FM_GPT.CHAT_ARTIFACTS
            WHERE CONVERSATION_ID = ?
              AND TURN_ID = ?
            ORDER BY CREATED_AT
        """, params=[conversation_id, turn_id]).collect()

        tables = []
        image_paths = []
        code_snippet = []

        for a in artifacts:
            artifact_type = a["ARTIFACT_TYPE"]
            artifact = a["ARTIFACT_DATA"]

            # Normalize Snowflake VARIANT
            if artifact is None:
                continue

            if isinstance(artifact, str):
                try:
                    artifact = json.loads(artifact)
                except Exception:
                    continue

            # === TABLE ===
            if artifact_type == "TABLE":
                if isinstance(artifact, list):
                    try:
                        df = pd.DataFrame(artifact)
                        if not df.empty:
                            tables.append(df)
                    except Exception:
                        pass

            # === CODE ===
            elif artifact_type == "CODE":
                if isinstance(artifact, dict) and "code" in artifact:
                    code_snippet.append(artifact["code"])

            # === IMAGE REF ===
            elif artifact_type == "IMAGE_REF":
                if isinstance(artifact, dict) and "stage_path" in artifact:
                    image_paths.append(artifact["stage_path"])

        # Push message
        st.session_state.messages.append({
            "role": "assistant",
            "type": "fm_result",
            "result": tables[0] if tables else None,
            "fig_paths": image_paths,
            "code_snippet": code_snippet,
            "explanation": t["CONTENT"],
        })

# ----------------------------------------------------------
# SIDEBAR
# ----------------------------------------------------------
with st.sidebar:
    
    st.header("💬 Chat History")

    if st.button("➕ New Chat"):
        cid = str(uuid.uuid4())
        session.sql("""
            INSERT INTO FM_GPT.CHAT_CONVERSATIONS
            (CONVERSATION_ID, TITLE, CREATED_AT, UPDATED_AT, USER_EMAIL)
            VALUES (?, 'New Chat', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,?)
        """, params=[cid, email]).collect()
        st.session_state.conversation_id = cid
        st.session_state.messages = []
        st.rerun()

    rows = session.sql("""
        SELECT CONVERSATION_ID, TITLE
        FROM FM_GPT.CHAT_CONVERSATIONS
        WHERE USER_EMAIL = ?
        ORDER BY UPDATED_AT DESC
    """, params=[email]).collect()

    for r in rows:
        if st.button(r["TITLE"], key=r["CONVERSATION_ID"]):
            load_conversation(session, r["CONVERSATION_ID"], email)
            st.rerun()

# ----------------------------------------------------------
# RENDER MESSAGE
# ----------------------------------------------------------
def render_message(msg):
    with st.chat_message(msg["role"]):

        # USER message
        if msg["type"] == "text":
            st.markdown(msg["content"])
            return

        # TABLE
        result_df = msg.get("result")
        if isinstance(result_df, pd.DataFrame) and not result_df.empty:
            st.dataframe(result_df, use_container_width=True)

        # IMAGES
        for path in msg.get("fig_paths", []):
            render_image_from_stage(path)

        # SINGLE CODE SNIPPET
        code_snippet = msg.get("code_snippet")
        if code_snippet:
            with st.expander("🔧 Generated Python Code"):
                st.code(code_snippet, language="python")

        # EXPLANATION
        explanation = msg.get("explanation")
        if explanation:
            st.markdown(explanation)

# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------

def main():
    # email = st.text_input('Enter your email').strip().lower()
    if email in exception_email:
        flag = 1
    else:
        flag = 0
    load_data()
    init_state()

    # Render full history
    for msg in st.session_state.messages:
        render_message(msg)

    prompt = st.chat_input("Ask anything about Funnel Management…")
    if not prompt:
        return

    # Conversation init
    if st.session_state.conversation_id is None:
        cid = str(uuid.uuid4())
        title = generate_chat_title(prompt)
        session.sql("""
            INSERT INTO FM_GPT.CHAT_CONVERSATIONS
            (CONVERSATION_ID, TITLE, CREATED_AT, UPDATED_AT, USER_EMAIL)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,?)
        """, params=[cid, title, email]).collect()
        st.session_state.conversation_id = cid

    cid = st.session_state.conversation_id

    # User turn
    user_msg = {"role": "user", "type": "text", "content": prompt}
    st.session_state.messages.append(user_msg)
    render_message(user_msg)
    save_turn(session, cid, "user", prompt)

    # Tools



    agent = create_agent(
        model=llm,
        tools=[
            FM_Misc_Tool,
    FM_Contact_Tool
        ],
        system_prompt=f"The logged-in user email is: {email} that uses {flag}"
        "You are an expert Funnel Management assistant. "
    "You MUST always use one of the provided tools to answer the user's question. "
    "Never respond directly. "
    "Call exactly ONE tool by choosing the best one for the question."
    )
    with st.chat_message("assistant"):
        with st.spinner("🤖 Analyzing your question and running the analysis..."):

            key = execution_key(prompt, cid)

            # if st.session_state.get("last_executed_key") != key:

            #     st.session_state["last_executed_key"] = key

            # agent_result = agent.invoke({
            #     "messages": [{"role": "user", "content": prompt}],
            #     "email" :email
            # })

            agent_result = agent.invoke({
        "messages": [{"role": "user", "content": prompt}],
        "email": email
    })
            #     st.session_state["last_agent_result"] = agent_result

            # else:
            #     agent_result = st.session_state["last_agent_result"]

    # ----------------------------
    # SAFE TOOL EXTRACTION
    # ----------------------------
    tool_out = None
    for m in agent_result.get("messages", []):
        if isinstance(m, ToolMessage):
            tool_out = parse_tool_output(m.content)
            break  # first valid tool only

    if not tool_out or tool_out.get("status") != "ok":
        st.error("❌ Analysis failed.")
    else:
        rows = tool_out.get("result", [])
        cols = tool_out.get("columns")
        stage_path = tool_out.get("chart_stage_path")
        oppt_json =   tool_out.get("oppt_json") if tool_out.get("oppt_json") is not None else ''
        code = tool_out.get("code")

        df = pd.DataFrame(rows)

        if code:
            st.code(code)

        if cols:
            df = df.reindex(columns=cols)

        st.success("✅ Analysis complete")


        if not df.empty:
            st.dataframe(df, width = 'stretch')

        if stage_path:
            render_image_from_stage(stage_path)

        if oppt_json:
            row_data = rows   # row matching contact id

            summary_prompt = f"""
        Create a detailed but concise human-readable summary for this contact.

        =====================
        CONTACT ROW DATA
        =====================
        {row_data}

    

        =====================
        Instructions
        =====================
        Write a clean business summary including:
        - total opportunities

        - amount won
        - amount lost
        - win rate
        - list won deals (product + amount)
        - list lost deals (product + amount)
        - list open deals with product + amount + stage
        - key products associated with the contact

        Make the tone professional and accurate.
        Do not invent data.
        Only use what is provided.
        """

            crm_summary = llm.invoke(summary_prompt)
            st.subheader("📄 Contact Summary")
            st.write(crm_summary.content)
        
            # row_data = rows[0] if rows else {}
            row_data = rows

            # name = row_data.get("name", "")
            # account = row_data.get("account_name", "")
            # country = row_data.get("country", "")
            name = df['contact name'].iloc[0]
            account = df['account name'].iloc[0]
            country = 'India'
            st.write(f'Details are.. {name}, {account}, {country}')
            web_out = simple_web_agent(account, 'India')
            # web_summary = azure_abm_websearch(name, account, country)

            # st.write(web_out)

            sales_intel = llm.invoke(f"""
            Combine CRM history and public web info to create:
            1. Contact summary
            2. Opportunity signals
            3. How to talk to them - While talking to them include the context of CRM history as well.
            4. What not to say
            5. A 20-second cold-call opener
            6. Also include the open opportunities and give me opportunity specific conversation that sales reps can have with the customer
                                     to move the opportunity forward or close the deal.

            CRM Data:
            {row_data}

            Web Intelligence:
            {web_out["summary"]}

            Provide crisp, actionable guidance.
            """).content

            st.write(sales_intel)

        # ----------------------------
        # PERSIST (only on fresh run)
        # ----------------------------
        if st.session_state.get("persisted_key") != key:

            st.session_state["persisted_key"] = key

            assistant_turn_id = save_turn(
                session, cid, "assistant", "Analysis complete"
            )

            if not df.empty:
                store_table_artifact(session, cid, assistant_turn_id, df)

            if stage_path:
                store_image_ref(session, cid, assistant_turn_id, stage_path)

            if code:
                store_code_artifact(session, cid, assistant_turn_id, code)

            st.session_state.messages.append({
                "role": "assistant",
                "type": "fm_result",
                "result": df,
                "code_snippet":code,
                "fig_paths": [stage_path] if stage_path else [],
                "explanation": "Analysis complete",
            })


if __name__ == "__main__":
    main()
