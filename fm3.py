import hashlib, os, json, uuid, tempfile
from datetime import datetime, timezone
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import parse_json
from langchain_openai import AzureChatOpenAI
from langchain.agents import create_agent
from langchain_core.messages import ToolMessage
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient

from fm_contact2 import FM_Contact_Tool

load_dotenv()

# ---------- CONFIG ----------
st.set_page_config(page_title='Funnel Management GPT', layout='wide')
st.title('📊 Funnel Management GPT')

# ---------- ENV ----------
ACCOUNT=os.getenv('ACCOUNT'); USER=os.getenv('USER'); PASSWORD=os.getenv('PASSWORD')
WAREHOUSE=os.getenv('WAREHOUSE'); DATABASE=os.getenv('DATABASE')
IMAGE_STAGE='@FM_GPT.CHAT_IMAGES_STAGE'

# ---------- LOGIN ----------
email = st.text_input('Enter email').strip().lower()
exception_email=['rsprasad@beckman.com','ldmello@beckman.com','pbopp@beckman.com']
flag = 1 if email in exception_email else 0

# ---------- LLM ----------
llm = AzureChatOpenAI(api_key=os.getenv('AZURE_OPENAI_API_KEY'), azure_endpoint=os.getenv('AZURE_ENDPOINT'), azure_deployment=os.getenv('LLM'), api_version=os.getenv('API_VERSION'), temperature=1)

# ---------- AZURE AGENT ----------
@st.cache_resource
def get_azure_agent_client():
    project_client = AIProjectClient(endpoint=os.getenv('AZURE_PROJECT_ENDPOINT'), credential=DefaultAzureCredential())
    return project_client.get_openai_client()

def azure_abm_websearch(name, company, country, question='Find publicly available business info'):
    try:
        client = get_azure_agent_client()
        prompt=f'''Person: {name}\nCompany: {company}\nCountry: {country}\n\n{question}\n\nReturn:\n- Public profile summary\n- Likely projects / initiatives\n- Recent company news\n- Business priorities\n- Useful sales insights\nUse only public web sources.'''
        response = client.responses.create(input=prompt, extra_body={'agent_reference':{'name':'ABMWebSearchAgent','version':'4','type':'agent_reference'}})
        return response.output_text
    except Exception as e:
        return f'Web search failed: {e}'

# ---------- HELPERS ----------
def get_session():
    return Session.builder.configs({'ACCOUNT':ACCOUNT,'USER':USER,'PASSWORD':PASSWORD,'WAREHOUSE':WAREHOUSE,'DATABASE':DATABASE}).create()
session=get_session()

def execution_key(prompt,cid): return hashlib.md5(f'{cid}::{prompt}'.encode()).hexdigest()
def generate_chat_title(prompt): return ' '.join(prompt.split()[:7])[:60]

def init_state():
    st.session_state.setdefault('conversation_id', None)
    st.session_state.setdefault('messages', [])

def save_turn(cid, role, content):
    tid=str(uuid.uuid4())
    session.sql('INSERT INTO FM_GPT.CHAT_TURNS (CONVERSATION_ID,TURN_ID,ROLE,CONTENT,USER_EMAIL,CREATED_AT) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)', params=[cid,tid,role,content,email]).collect()
    return tid

def store_table_artifact(cid, tid, df):
    payload=json.dumps(df.to_dict('records'))
    created_at=datetime.now(timezone.utc).replace(tzinfo=None)
    session.create_dataframe([[cid,tid,'TABLE',payload,created_at,email]], schema=['CONVERSATION_ID','TURN_ID','ARTIFACT_TYPE','ARTIFACT_DATA','CREATED_AT','USER_EMAIL']).select('CONVERSATION_ID','TURN_ID','ARTIFACT_TYPE',parse_json('ARTIFACT_DATA').alias('ARTIFACT_DATA'),'CREATED_AT','USER_EMAIL').write.mode('append').save_as_table('FM_GPT.CHAT_ARTIFACTS')

def render_message(msg):
    with st.chat_message(msg['role']):
        if msg['type']=='text': st.markdown(msg['content']); return
        if isinstance(msg.get('result'), pd.DataFrame): st.dataframe(msg['result'], use_container_width=True)
        if msg.get('explanation'): st.markdown(msg['explanation'])

# ---------- DATA ----------
@st.cache_data
def load_data():
    df=pd.read_excel('ABM_Data.xlsx', sheet_name='Common')
    df.columns=df.columns.str.lower()
    df.to_pickle('df_misc_funnel.pkl')

# ---------- MAIN ----------
def main():
    load_data(); init_state()
    for msg in st.session_state.messages: render_message(msg)
    prompt=st.chat_input('Ask anything about Funnel Management…')
    if not prompt: return

    if st.session_state.conversation_id is None:
        cid=str(uuid.uuid4())
        session.sql('INSERT INTO FM_GPT.CHAT_CONVERSATIONS (CONVERSATION_ID,TITLE,CREATED_AT,UPDATED_AT,USER_EMAIL) VALUES (?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?)', params=[cid,generate_chat_title(prompt),email]).collect()
        st.session_state.conversation_id=cid
    cid=st.session_state.conversation_id

    user_msg={'role':'user','type':'text','content':prompt}
    st.session_state.messages.append(user_msg); render_message(user_msg); save_turn(cid,'user',prompt)

    from fm_misc import FM_Misc_Tool
    agent=create_agent(model=llm, tools=[FM_Misc_Tool, FM_Contact_Tool], system_prompt=f'The logged-in user email is: {email} that uses {flag}. Always use one tool.')

    with st.chat_message('assistant'):
        with st.spinner('🤖 Analyzing...'):
            result=agent.invoke({'messages':[{'role':'user','content':prompt}], 'email':email})

    tool_out=None
    for m in result.get('messages',[]):
        if isinstance(m, ToolMessage):
            tool_out = json.loads(m.content) if isinstance(m.content,str) else m.content
            break
    if not tool_out or tool_out.get('status')!='ok':
        st.error('❌ Analysis failed'); return

    rows=tool_out.get('result',[]); cols=tool_out.get('columns'); df=pd.DataFrame(rows)
    if cols: df=df.reindex(columns=cols)
    st.success('✅ Analysis complete')
    if not df.empty: st.dataframe(df, use_container_width=True)

    if tool_out.get('oppt_json'):
        name=df['contact name'].iloc[0]; account=df['account name'].iloc[0]; country='India'
        web_summary=azure_abm_websearch(name, account, country)
        sales=llm.invoke(f'CRM Data: {rows}\nWeb Intelligence: {web_summary}\nGive contact summary, opportunity signals, how to talk, what not to say, cold-call opener.').content
        st.markdown(sales)

    aid=save_turn(cid,'assistant','Analysis complete')
    if not df.empty: store_table_artifact(cid,aid,df)
    st.session_state.messages.append({'role':'assistant','type':'fm_result','result':df,'explanation':'Analysis complete'})

if __name__=='__main__':
    main()
